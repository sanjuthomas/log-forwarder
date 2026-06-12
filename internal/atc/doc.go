// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

// Package atc registers and deregisters forwarder instances with log-forwarder-atc.
//
// The forwarder calls the controller only at process boundaries:
//   - PUT after startup (hostname, metrics port, process ID, UTC timestamp)
//   - DELETE before shutdown
//
// While running, the forwarder does not contact ATC. The controller polls
// GET /health and GET /ready on the registered host and port instead.
//
// Registration failures are logged at WARN and do not block log forwarding.
package atc
