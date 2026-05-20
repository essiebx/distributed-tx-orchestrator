# Distributed Financial Transaction Orchestrator (Saga Pattern)

A production-grade, distributed transaction orchestrator implementing the **Saga Pattern** using Scala 3, ZIO 2, and event-driven choreography via Kafka. This engine reliably manages complex multi-step workflows (such as banking transfers or e-commerce checkouts) across isolated microservices, enforcing eventual consistency via automated compensating actions upon failure.

## 1. Architecture Overview

The system uses an **Orchestrator-driven Saga** design. Rather than letting individual microservices guess the global state, a centralized, reactive engine manages transaction lifecycles, monitors progress, and handles rollback routing over an asynchronous message backbone.

   [ React Frontend ] (Vercel / Netlify)
           │
           ▼ REST / Server-Sent Events (SSE)

[ ZIO Saga Orchestrator Service ] (Render / Fly.io)
│
┌────────┴────────┐ Kafka (Redpanda Local / Upstash Cloud)
▼                 ▼
[Order Service]   [Payment Service] (Mocked via Isolated ZIO Fibers)


### Core Architecture Components
*   **The Saga Log:** Implemented via PostgreSQL to record immutable transitions (`Started`, `StepSucceeded`, `Compensating`, `Failed`). If the orchestrator node crashes mid-transaction, it reads the log upon recovery to resume or compensate open sagas.
*   **Asynchronous Messaging:** Low-latency event streaming via Kafka handles communication between the orchestrator and microservices, ensuring loose temporal coupling.
*   **Real-Time Telemetry:** Updates are streamed directly from the engine to the frontend UI via Server-Sent Events (SSE) over HTTP, visualizing step execution and rollbacks dynamically.

---

## 2. Advanced Functional Programming Concepts Utilized

This project bypasses traditional Enterprise Java patterns in favor of type-safe, declarative functional programming:

*   **Virtual Threading (Fibers):** Utilizing ZIO's native fiber primitives to spawn lightweight, non-blocking loops per transaction. This allows the system to process thousands of concurrent sagas with minimal memory footprint compared to OS-level thread pools.
*   **Atomic State Mutability (`Ref`):** Thread-safe tracking of in-flight compensations using concurrent atomic reference variables (`ZIO.Ref`), eliminating structural mutability blocks or explicit synchronization locks.
*   **Monadic Composition:** Deeply nested workflow structures are flattened into readable, declarative linear steps using Scala `for-comprehensions`, guaranteeing structural determinism.
*   **Declarative Resiliency (`Schedule`):** Transient network and infrastructure errors are handled via ZIO's `Schedule` primitive, providing elegant, single-line retry behaviors (e.g., exponential backoff with jitter).

---

## 3. Technology Stack

### Backend
*   **Language:** Scala 3.x
*   **Effect System:** ZIO 2.x (Core, Streams, JSON)
*   **HTTP/Streaming HTTP:** ZIO-HTTP
*   **Message Broker Client:** ZIO-Kafka
*   **Database Access:** Quill / JDBC (PostgreSQL Alpine)

### Frontend
*   **Framework:** React 18+ (TypeScript, Tailwind CSS)
*   **Streaming Client:** EventSource API (Native SSE)

### Infrastructure (Free Developer Tiers)
*   **Local Engine:** Docker Compose, Redpanda (C++ drop-in Kafka alternative)
*   **Cloud Hosting:** Render / Fly.io (Backend), Vercel (Frontend)
*   **Managed Services:** Upstash (Serverless Kafka), Supabase / Neon (Serverless PostgreSQL)

---

## 4. Project Structure

The codebase is organized as an SBT multi-module monorepo alongside an isolated frontend application:

```text
├── build.sbt                # Multi-module Scala compilation layout
├── docker-compose.yml       # Dev infra (Redpanda, Postgres)
├── core-orchestrator/       # ZIO engine, Saga log database management, SSE streaming
│   └── src/main/scala/
├── shared-events/           # Shared transactional event schemas (Scala 3 Enums)
├── mock-services/           # Simulated independent service actors listening to Kafka topics
├── frontend/                # React dashboard for visual transaction tracking
│   ├── src/                 # Event-driven visual timeline components
│   └── package.json
└── README.md

5. Local Setup & Execution
Prerequisites

    Docker & Docker Compose

    SBT (Scala Build Tool)

    Node.js & npm (for the tracking dashboard)

Step 1: Spin up Local Infrastructure

Launch the lightweight message broker and the persistence database:
Bash

docker-compose up -d

Step 2: Run the Backend Orchestrator

Launch the SBT shell and start the core engine and mock services:
Bash

sbt
> project coreOrchestrator
> run

Step 3: Run the Dashboard UI

In a separate terminal, navigate to the frontend directory and start the local development server:
Bash

cd frontend
npm install
npm run dev

Open http://localhost:5173 to view the step-by-step transaction tracker.
