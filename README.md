# ROS TCP Endpoint

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Introduction

[ROS](https://www.ros.org/) package used to create an endpoint to accept ROS messages sent from a Unity scene using the [ROS TCP Connector](https://github.com/Unity-Technologies/ROS-TCP-Connector) scripts.

Instructions and examples on how to use this ROS package can be found on the [Unity Robotics Hub](https://github.com/Unity-Technologies/Unity-Robotics-Hub/blob/master/tutorials/ros_unity_integration/README.md) repository.

## ROS2 Action Support (Preview)

Version 0.8.0 introduces an experimental ROS2 action bridge so Unity experiences can send action goals, stream feedback, and finalize results through the same TCP session used for topics and services. The feature is off by default for older connectors; Unity clients must negotiate the `actions-preview` capability during the handshake and emit the following syscommands before sending the serialized ROS messages:

| SysCommand | Purpose | Next Payload |
|------------|---------|--------------|
| `__action_goal` | Unity → ROS: register an action goal (fields: `action_name`, `goal_id`) | `action_type.Goal` bytes |
| `__action_goal_response` | ROS → Unity: respond to a Unity-sent action goal (fields: `action_name`, `goal_id`, `accepted`, `ros_goal_id`, `message`) | None |
| `__action_feedback` | Unity → ROS: stream feedback (fields: `action_name`, `goal_id`) | `action_type.Feedback` bytes |
| `__action_result` | Unity → ROS: complete execution (fields: `action_name`, `goal_id`, `status`) | `action_type.Result` bytes |
| `__action_cancel` | Unity → ROS: cancel a previously-sent goal (fields: `action_name`, `goal_id`) | None |
| `__action_goal_request` | ROS → Unity: ROS node is delegating goal execution to Unity (fields: `action_name`, `goal_id`) | `action_type.Goal` bytes |
| `__action_cancel_request` | ROS → Unity: ROS node accepted a cancel request; Unity should stop work | None |

Unity connectors that register ROS action clients should emit the goal syscommand followed by the serialized goal payload. The endpoint will deserialize the message, forward it via `rclpy.action.ActionClient`, and propagate feedback/results back over TCP. Conversely, registering Unity-hosted action servers allows ROS clients to trigger gameplay logic: the endpoint wraps `ActionServer` and forwards execute/cancel events to Unity using `__action_goal_request`/`__action_cancel_request`. Feedback/results must be sent back with the matching `goal_id` so ROS goal handles stay in sync.

## Community and Feedback

The Unity Robotics projects are open-source and we encourage and welcome contributions.
If you wish to contribute, be sure to review our [contribution guidelines](CONTRIBUTING.md)
and [code of conduct](CODE_OF_CONDUCT.md).

## Support
For questions or discussions about Unity Robotics package installations or how to best set up and integrate your robotics projects, please create a new thread on the [Unity Robotics forum](https://forum.unity.com/forums/robotics.623/) and make sure to include as much detail as possible.

For feature requests, bugs, or other issues, please file a [GitHub issue](https://github.com/Unity-Technologies/ROS-TCP-Endpoint/issues) using the provided templates and the Robotics team will investigate as soon as possible.

For any other questions or feedback, connect directly with the
Robotics team at [unity-robotics@unity3d.com](mailto:unity-robotics@unity3d.com).

## License
[Apache License 2.0](LICENSE)
