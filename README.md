# 🗺️ Coordinates Project

A highly resilient, event-driven microservice pipeline built with Python and FastAPI. 
The system processes location data and manages user sign-ins across isolated containers using a local Apache Kafka event stream.

---

## 🏗️ System Architecture

The project splits responsibilities across 4 specialized services to ensure data security and fast, non-blocking performance:

*   **Service A (Gateway):** The main entry point. Validates sign-in requests, filters out duplicate IP submissions within a rolling 60-second window, and publishes unique events to Kafka.
*   **Service B (Data Custodian):** Holds exclusive access to the shared database files (`storage.json` and `users.json`). It manages thread-safe writes using async locks and calculates geographic data clusters.
*   **Service C (API Integrator):** Handles live third-party geolocation lookups using a sequential failover loop. It monitors external rate limits dynamically to protect your system from IP bans.
*   **Kafka Consumer:** Subscribes to backend sign-in events asynchronously to automatically register and log new user profiles in the database.

---

## ⚡ Key Core Features

*   **Sequential API Failover:** Queries a primary provider first. If it encounters a timeout or connection issue, it switches to a secondary backup provider within milliseconds.
*   **Rate-Limit Protection:** Automatically reads upstream tracking headers (`X-Rl` and `X-Ttl`) and locks outbound calls proactively if an external API limit is exhausted.
*   **Sliding Window Interception:** Monitors concurrent traffic and intercepts duplicate actions, returning a clean `{"signin": "duplicate"}` flag back to the client.
*   **Zero-Zookeeper Kafka setup:** Powered by a standalone Apache Kafka (`apache/kafka:4.3.1`) container operating cleanly in modern KRaft mode.

---

## 📡 API Route & Endpoint Guide

### 1. View Cached Locations
*   **URL:** `http://localhost:8081/`
*   **Method:** `GET`
*   **Description:** Displays all IP address resolutions currently saved in the `storage.json` file database. This serves as a quick dump to inspect all active geolocated records managed by Service B.

### 2. Live IP Resolution & Ingestion
*   **URL:** `http://localhost:8081/resolve-ip/1.1.1.1` *(Replace `1.1.1.1` with any target IP)*
*   **Method:** `GET`
*   **Description:** Checks for the geolocation of an IP. If it is already cached, it skips external lookup; if it is new, Service C resolves its coordinates and Service B adds it to `storage.json`.

### 3. Remove a Database Record
*   **URL:** `http://localhost:8081/delete/38fcf6f3` *(Replace `38fcf6f3` with a valid entry ID)*
*   **Method:** `GET`
*   **Description:** Deletes a specific geolocation record from `storage.json`. The unique 8-character record IDs can be found by inspecting your main location list (`http://localhost:8081/`).

### 4. Geographic Distance Clustering
*   **URL:** `http://localhost:8081/clusters?threshold=0.5` *(Adjust the `threshold` query value)*
*   **Method:** `GET`
*   **Description:** Groups your saved records into distinct clusters based on geographical distance. The `threshold` parameter defines the radius using Euclidean distance logic (where a threshold of `1` roughly equals 110 kilometers).

### 5. Asynchronous User Sign-In / Sign-Up
*   **URL:** `http://localhost:8081/docs#/default/user_signin_signin_post`
*   **Method:** `POST`
*   **Description:** Interactive documentation form to send a login payload. Service A checks the 60-second duplicate window and drops an event onto Kafka. The background Kafka Consumer reads the event and asks Service B to process it as a *Sign-In* (if the email exists) or a brand new *Sign-Up* record in `users.json`.

---

## 🚀 Quick Start

### 1. Boot the Stack
Run the following single command in your root project directory to build and spin up all containers simultaneously:

```bash
docker-compose up --build
```

### 2. Test the API Interface
Once the services are active, open your web browser and navigate to the built-in interactive documentation panel:

📌 **`http://localhost:8081/docs`**

From here, you can execute test payloads, trigger the validation layers, and watch the event logs flow live across your microservice terminal!

---

## 📊 System Component Diagram

```text
                        ┌──────────────────┐
                        │ Client / Browser │
                        └────────┬─────────┘
                                 │
                                 ▼ (Port 8081)
                        ┌──────────────────┐
                        │    Service A     │ ◀───┐ (Check Cache)
                        └────────┬─────────┘     │
                                 │               ▼
        (Publish Sign-In)        │        ┌──────────────┐
        ─────────────────────────┼───────▶│  Service B   │ (Data\memory manager)
        │                        │        └───────┬──────┘
        ▼                        ▼                │
┌──────────────┐          ┌──────────────┐        ├─▶ [storage.json]
│ Kafka Broker │─────────▶│  Service C   │        │
└────────┬─────┘          └──────────────┘        └─▶ [users.json]
         │                (API Integrator)
         ▼
┌──────────────┐
│Kafka Consumer│
└──────────────┘
```
