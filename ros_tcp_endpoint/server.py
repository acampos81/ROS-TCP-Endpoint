#  Copyright 2020 Unity Technologies
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import rclpy
import socket
import json
import sys
import threading
import importlib

from rclpy.node import Node
from rclpy.parameter import Parameter
from rclpy.executors import MultiThreadedExecutor
from rclpy.serialization import deserialize_message

from .tcp_sender import UnityTcpSender
from .client import ClientThread
from .subscriber import RosSubscriber
from .publisher import RosPublisher
from .service import RosService
from .unity_service import UnityService
from .action_client import RosActionClient
from .unity_action import UnityActionServer


class TcpServer(Node):
    """
    Initializes ROS node and TCP server.
    """

    def __init__(self, node_name, buffer_size=1024, connections=10, tcp_ip=None, tcp_port=None):
        """
        Initializes ROS node and class variables.

        Args:
            node_name:               ROS node name for executing code
            buffer_size:             The read buffer size used when reading from a socket
            connections:             Max number of queued connections. See Python Socket documentation
        """
        super().__init__(node_name)

        self.declare_parameter("ROS_IP", "0.0.0.0")
        self.declare_parameter("ROS_TCP_PORT", 10000)

        if tcp_ip:
            self.loginfo("Using ROS_IP override from constructor: {}".format(tcp_ip))
            self.tcp_ip = tcp_ip
        else:
            self.tcp_ip = self.get_parameter("ROS_IP").get_parameter_value().string_value

        if tcp_port:
            self.loginfo("Using ROS_TCP_PORT override from constructor: {}".format(tcp_port))
            self.tcp_port = tcp_port
        else:
            self.tcp_port = self.get_parameter("ROS_TCP_PORT").get_parameter_value().integer_value

        self.unity_tcp_sender = UnityTcpSender(self)

        self.node_name = node_name
        self.publishers_table = {}
        self.subscribers_table = {}
        self.ros_services_table = {}
        self.unity_services_table = {}
        self.ros_action_clients = {}
        self.unity_action_servers = {}
        self.buffer_size = buffer_size
        self.connections = connections
        self.syscommands = SysCommands(self)
        self.pending_srv_id = None
        self.pending_srv_is_request = False
        self.pending_action = None

    def start(self, publishers=None, subscribers=None):
        if publishers is not None:
            self.publishers_table = publishers
        if subscribers is not None:
            self.subscribers_table = subscribers
        server_thread = threading.Thread(target=self.listen_loop)
        # Exit the server thread when the main thread terminates
        server_thread.daemon = True
        server_thread.start()

    def listen_loop(self):
        """
            Creates and binds sockets using TCP variables then listens for incoming connections.
            For each new connection a client thread will be created to handle communication.
        """
        self.loginfo("Starting server on {}:{}".format(self.tcp_ip, self.tcp_port))
        tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_server.bind((self.tcp_ip, self.tcp_port))

        while True:
            tcp_server.listen(self.connections)

            try:
                (conn, (ip, port)) = tcp_server.accept()
                ClientThread(conn, self, ip, port).start()
            except socket.timeout as err:
                self.logerr("ros_tcp_endpoint.TcpServer: socket timeout")

    def send_unity_error(self, error):
        self.unity_tcp_sender.send_unity_error(error)

    def send_unity_message(self, topic, message):
        self.unity_tcp_sender.send_unity_message(topic, message)

    def send_unity_service(self, topic, service_class, request):
        return self.unity_tcp_sender.send_unity_service_request(topic, service_class, request)

    def send_unity_service_response(self, srv_id, data):
        self.unity_tcp_sender.send_unity_service_response(srv_id, data)

    def cancel_ros_action_goal(self, action_name, goal_id):
        action_client = self.ros_action_clients.get(action_name)
        if action_client is None:
            self.send_unity_error(
                "Cannot cancel goal {} because action '{}' is not registered.".format(
                    goal_id, action_name
                )
            )
            return
        self.loginfo("Forwarding cancel for action {} goal {}".format(action_name, goal_id))
        action_client.cancel_goal(goal_id)

    def handle_syscommand(self, topic, data):
        function = getattr(self.syscommands, topic[2:])
        if function is None:
            self.send_unity_error("Don't understand SysCommand.'{}'".format(topic))
        else:
            message_json = data.decode("utf-8")[:-1]
            params = json.loads(message_json)
            function(**params)

    def loginfo(self, text):
        self.get_logger().info(text)

    def logwarn(self, text):
        self.get_logger().warning(text)

    def logerr(self, text):
        self.get_logger().error(text)

    def setup_executor(self):
        """
            Since rclpy.spin() is a blocking call the server needed a way
            to spin all of the relevant nodes at the same time.

            MultiThreadedExecutor allows us to set the number of threads
            needed as well as the nodes that need to be spun.
        """
        num_threads = (
            len(self.publishers_table.keys())
            + len(self.subscribers_table.keys())
            + len(self.ros_services_table.keys())
            + len(self.unity_services_table.keys())
            + len(self.ros_action_clients.keys())
            + len(self.unity_action_servers.keys())
            + 1
        )
        executor = MultiThreadedExecutor(num_threads)

        executor.add_node(self)

        for ros_node in self.publishers_table.values():
            executor.add_node(ros_node)
        for ros_node in self.subscribers_table.values():
            executor.add_node(ros_node)
        for ros_node in self.ros_services_table.values():
            executor.add_node(ros_node)
        for ros_node in self.unity_services_table.values():
            executor.add_node(ros_node)
        for ros_node in self.ros_action_clients.values():
            executor.add_node(ros_node)
        for ros_node in self.unity_action_servers.values():
            executor.add_node(ros_node)

        self.executor = executor
        executor.spin()

    def unregister_node(self, old_node):
        if old_node is not None:
            old_node.unregister()
            if self.executor is not None:
                self.executor.remove_node(old_node)

    def destroy_nodes(self):
        """
            Clean up all of the nodes
        """
        for ros_node in self.publishers_table.values():
            ros_node.destroy_node()
        for ros_node in self.subscribers_table.values():
            ros_node.destroy_node()
        for ros_node in self.ros_services_table.values():
            ros_node.destroy_node()
        for ros_node in self.unity_services_table.values():
            ros_node.destroy_node()
        for ros_node in self.ros_action_clients.values():
            ros_node.destroy_node()
        for ros_node in self.unity_action_servers.values():
            ros_node.destroy_node()

        self.destroy_node()


class SysCommands:
    def __init__(self, tcp_server):
        self.tcp_server = tcp_server

    def _normalize_action_name(self, name: str) -> str:
        """Return a canonical action/topic name: single leading '/', no trailing '/', collapse repeats."""
        if name is None:
            return name
        # Strip whitespace and collapse multiple slashes
        parts = [p for p in str(name).strip().split('/') if p]
        normalized = '/' + '/'.join(parts) if parts else ''
        return normalized

    def subscribe(self, topic, message_name):
        if topic == "":
            self.tcp_server.send_unity_error(
                "Can't subscribe to a blank topic name! SysCommand.subscribe({}, {})".format(
                    topic, message_name
                )
            )
            return

        message_class = self.resolve_message_name(message_name)
        if message_class is None:
            self.tcp_server.send_unity_error(
                "SysCommand.subscribe - Unknown message class '{}'".format(message_name)
            )
            return

        old_node = self.tcp_server.subscribers_table.get(topic)
        if old_node is not None:
            self.tcp_server.unregister_node(old_node)

        new_subscriber = RosSubscriber(topic, message_class, self.tcp_server)
        self.tcp_server.subscribers_table[topic] = new_subscriber
        if self.tcp_server.executor is not None:
            self.tcp_server.executor.add_node(new_subscriber)

        self.tcp_server.loginfo("RegisterSubscriber({}, {}) OK".format(topic, message_class))

    def remove_subscriber(self, topic):
        if topic == "":
            self.tcp_server.send_unity_error(
                "Can't unsubscribe from a blank topic name! SysCommand.remove_subscriber({})".format(topic)
            )
            return

        old_node = self.tcp_server.subscribers_table.get(topic)
        if old_node is None:
            self.tcp_server.send_unity_error(
                "SysCommand.remove_subscriber - Topic '{}' is not registered".format(topic)
            )
            return

        self.tcp_server.unregister_node(old_node)
        del self.tcp_server.subscribers_table[topic]

        self.tcp_server.loginfo("UnregisterSubscriber({}) OK".format(topic))

    def publish(self, topic, message_name, queue_size=10, latch=False):
        if topic == "":
            self.tcp_server.send_unity_error(
                "Can't publish to a blank topic name! SysCommand.publish({}, {})".format(
                    topic, message_name
                )
            )
            return

        message_class = self.resolve_message_name(message_name)
        if message_class is None:
            self.tcp_server.send_unity_error(
                "SysCommand.publish - Unknown message class '{}'".format(message_name)
            )
            return

        old_node = self.tcp_server.publishers_table.get(topic)
        if old_node is not None:
            self.tcp_server.unregister_node(old_node)

        new_publisher = RosPublisher(topic, message_class, queue_size=queue_size, latch=latch)

        self.tcp_server.publishers_table[topic] = new_publisher
        if self.tcp_server.executor is not None:
            self.tcp_server.executor.add_node(new_publisher)

        self.tcp_server.loginfo("RegisterPublisher({}, {}) OK".format(topic, message_class))

    def ros_service(self, topic, message_name):
        if topic == "":
            self.tcp_server.send_unity_error(
                "RegisterRosService({}, {}) - Can't register a blank topic name!".format(
                    topic, message_name
                )
            )
            return
        message_class = self.resolve_message_name(message_name, "srv")
        if message_class is None:
            self.tcp_server.send_unity_error(
                "RegisterRosService({}, {}) - Unknown service class '{}'".format(
                    topic, message_name, message_name
                )
            )
            return

        old_node = self.tcp_server.ros_services_table.get(topic)
        if old_node is not None:
            self.tcp_server.unregister_node(old_node)

        new_service = RosService(topic, message_class)

        self.tcp_server.ros_services_table[topic] = new_service
        if self.tcp_server.executor is not None:
            self.tcp_server.executor.add_node(new_service)

        self.tcp_server.loginfo("RegisterRosService({}, {}) OK".format(topic, message_class))

    def unity_service(self, topic, message_name):
        if topic == "":
            self.tcp_server.send_unity_error(
                "RegisterUnityService({}, {}) - Can't register a blank topic name!".format(
                    topic, message_name
                )
            )
            return

        message_class = self.resolve_message_name(message_name, "srv")
        if message_class is None:
            self.tcp_server.send_unity_error(
                "RegisterUnityService({}, {}) - Unknown service class '{}'".format(
                    topic, message_name, message_name
                )
            )
            return

        old_node = self.tcp_server.unity_services_table.get(topic)
        if old_node is not None:
            self.tcp_server.unregister_node(old_node)

        new_service = UnityService(str(topic), message_class, self.tcp_server)

        self.tcp_server.unity_services_table[topic] = new_service
        if self.tcp_server.executor is not None:
            self.tcp_server.executor.add_node(new_service)

        self.tcp_server.loginfo("RegisterUnityService({}, {}) OK".format(topic, message_class))

    def ros_action(self, action_name, action_type):
        if action_name == "":
            self.tcp_server.send_unity_error(
                "RegisterRosAction({}, {}) - Can't register a blank action name!".format(
                    action_name, action_type
                )
            )
            return
        original_name = action_name
        action_name = self._normalize_action_name(action_name)
        if original_name != action_name:
            self.tcp_server.loginfo(
                "RegisterRosAction: normalized action name '{}' -> '{}'".format(original_name, action_name)
            )
        action_class = self.resolve_message_name(action_type, "action")
        if action_class is None:
            self.tcp_server.send_unity_error(
                "RegisterRosAction({}, {}) - Unknown action class '{}'".format(
                    action_name, action_type, action_type
                )
            )
            return

        old_node = self.tcp_server.ros_action_clients.get(action_name)
        if old_node is not None:
            self.tcp_server.unregister_node(old_node)

        new_client = RosActionClient(action_name, action_class, self.tcp_server)
        self.tcp_server.ros_action_clients[action_name] = new_client
        if self.tcp_server.executor is not None:
            self.tcp_server.executor.add_node(new_client)

        self.tcp_server.loginfo("RegisterRosAction({}, {}) OK".format(action_name, action_class))

    def unity_action(self, action_name, action_type):
        if action_name == "":
            self.tcp_server.send_unity_error(
                "RegisterUnityAction({}, {}) - Can't register a blank action name!".format(
                    action_name, action_type
                )
            )
            return
        original_name = action_name
        action_name = self._normalize_action_name(action_name)
        if original_name != action_name:
            self.tcp_server.loginfo(
                "RegisterUnityAction: normalized action name '{}' -> '{}'".format(original_name, action_name)
            )
        action_class = self.resolve_message_name(action_type, "action")
        if action_class is None:
            self.tcp_server.send_unity_error(
                "RegisterUnityAction({}, {}) - Unknown action class '{}'".format(
                    action_name, action_type, action_type
                )
            )
            return

        old_node = self.tcp_server.unity_action_servers.get(action_name)
        if old_node is not None:
            self.tcp_server.unregister_node(old_node)

        new_server = UnityActionServer(action_name, action_class, self.tcp_server)
        self.tcp_server.unity_action_servers[action_name] = new_server
        if self.tcp_server.executor is not None:
            self.tcp_server.executor.add_node(new_server)

        self.tcp_server.loginfo("RegisterUnityAction({}, {}) OK".format(action_name, action_class))

    def action_goal(self, action_name, goal_id):
        # Normalize provided action name for consistent lookup
        original_name = action_name
        action_name = self._normalize_action_name(action_name)
        if original_name != action_name:
            self.tcp_server.loginfo(
                "action_goal: normalized action name '{}' -> '{}'".format(original_name, action_name)
            )
        # Defer goal delivery even if the action client is not yet registered.
        # This avoids transient ordering issues where __ros_action arrives slightly
        # after __action_goal on the endpoint. We'll check again at delivery time.
        if action_name not in self.tcp_server.ros_action_clients:
            self.tcp_server.logwarn(
                "Action goal received before action '{}' registered; deferring until payload arrives.".format(
                    action_name
                )
            )
        self._set_pending_action(action_name, goal_id, "goal_to_ros")

    def action_feedback(self, action_name, goal_id):
        original_name = action_name
        action_name = self._normalize_action_name(action_name)
        if original_name != action_name:
            self.tcp_server.loginfo(
                "action_feedback: normalized action name '{}' -> '{}'".format(original_name, action_name)
            )
        if action_name not in self.tcp_server.unity_action_servers:
            self.tcp_server.send_unity_error(
                "Action feedback received for unknown Unity action '{}'".format(action_name)
            )
            return
        self._set_pending_action(action_name, goal_id, "feedback_to_ros")

    def action_result(self, action_name, goal_id, status=0):
        original_name = action_name
        action_name = self._normalize_action_name(action_name)
        if original_name != action_name:
            self.tcp_server.loginfo(
                "action_result: normalized action name '{}' -> '{}'".format(original_name, action_name)
            )
        if action_name not in self.tcp_server.unity_action_servers:
            self.tcp_server.send_unity_error(
                "Action result received for unknown Unity action '{}'".format(action_name)
            )
            return
        self._set_pending_action(action_name, goal_id, "result_to_ros", status=status)

    def action_cancel(self, action_name, goal_id):
        original_name = action_name
        action_name = self._normalize_action_name(action_name)
        if original_name != action_name:
            self.tcp_server.loginfo(
                "action_cancel: normalized action name '{}' -> '{}'".format(original_name, action_name)
            )
        if action_name in self.tcp_server.ros_action_clients:
            self.tcp_server.cancel_ros_action_goal(action_name, goal_id)
        else:
            self.tcp_server.send_unity_error(
                "Action cancel received for unknown ROS action '{}'".format(action_name)
            )

    def response(self, srv_id):  # the next message is a service response
        self.tcp_server.pending_srv_id = srv_id
        self.tcp_server.pending_srv_is_request = False

    def request(self, srv_id):  # the next message is a service request
        self.tcp_server.pending_srv_id = srv_id
        self.tcp_server.pending_srv_is_request = True

    def topic_list(self):
        self.tcp_server.unity_tcp_sender.send_topic_list()

    def resolve_message_name(self, name, extension_hint=None):
        import importlib

        try:
            parts = name.split('/')
            if len(parts) == 3:
                package_name, extension, class_name = parts
            elif len(parts) == 2:
                package_name, class_name = parts
                extension = extension_hint or "msg"
            else:
                raise ValueError(f"Invalid ROS type string: {name}")

            if extension not in ["msg", "srv", "action"]:
                extension = "msg"

            # Construct proper module name
            # For all types, import from <package>.<extension> and get the class by name
            mod_name = f"{package_name}.{extension}"

            try:
                module = importlib.import_module(mod_name)
                msg_class = getattr(module, class_name)
                return msg_class
            except (ModuleNotFoundError, AttributeError):
                # If not found in the specified extension, try other extensions
                # This handles cases where action goal/result/feedback are registered with their ROS message name
                # but the actual class lives in the action module
                if extension == "msg":
                    for fallback_ext in ["action", "srv"]:
                        try:
                            fallback_mod_name = f"{package_name}.{fallback_ext}"
                            fallback_module = importlib.import_module(fallback_mod_name)
                            fallback_class = getattr(fallback_module, class_name)
                            
                            # Special handling for action types:
                            # If we found an action wrapper class (which cannot be instantiated),
                            # we need to return the Goal/Result/Feedback subclass instead.
                            # Try to instantiate it - if it fails with NotImplementedError, 
                            # it's an action wrapper class, so extract Goal subclass as fallback
                            if fallback_ext == "action":
                                try:
                                    # Try to instantiate to check if it's a wrapper class
                                    test_instance = fallback_class()
                                    # If successful, return the class as-is
                                    return fallback_class
                                except NotImplementedError:
                                    # It's an action wrapper class, extract the Goal subclass
                                    action_goal = getattr(fallback_class, "Goal", None)
                                    if action_goal is not None:
                                        self.tcp_server.loginfo(f"Resolved {name} to action Goal type (wrapper class detected)")
                                        return action_goal
                                    # If no Goal subclass, re-raise the error
                                    raise
                            
                            self.tcp_server.loginfo(f"Resolved {name} from {fallback_ext} module instead of {extension}")
                            return fallback_class
                        except (ModuleNotFoundError, AttributeError):
                            continue
                # If we get here, nothing worked
                raise

        except Exception as e:
            self.tcp_server.logerr(f"Failed to resolve {name}: {e}")
            return None


    def _set_pending_action(self, action_name, goal_id, phase, status=None):
        self.tcp_server.pending_action = {
            "action_name": action_name,
            "goal_id": goal_id,
            "phase": phase,
            "status": status,
        }
