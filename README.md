# Distributed Financial Transaction Orchestrator (Saga Pattern)

A production-grade, distributed transaction orchestrator implementing the Saga Pattern using Scala 3, ZIO 2, and event-driven choreography via Kafka. This engine reliably manages complex multi-step financial transactions across multiple microservices while maintaining consistency through automated compensating transactions.

## 1. Architecture Overview

The system uses an Orchestrator-driven Saga design pattern. Rather than allowing individual microservices to manage global state independently, a centralized, reactive engine manages transaction lifecycles, monitors compensation chains, and ensures eventual consistency across all participants.

```
[ React Frontend ] (Vercel / Netlify)
        |
        | REST / Server-Sent Events (SSE)
        |
        v
[ ZIO Saga Orchestrator Service ] (Render / Fly.io)
        |
        ├─ Kafka (Redpanda Local / Upstash Cloud)
        |
        ├──────────────────┬──────────────────┐
        |                  |
        v                  v
[ Order Service ]    [ Payment Service ]
(Mocked via Isolated ZIO Fibers)
```

### Core Architecture Components

* **Saga Log**: Implemented via PostgreSQL to record immutable state transitions (Started, StepSucceeded, Compensating, Failed). If the orchestrator node crashes during transaction processing, it reads the log and recovers from the last consistent state.
* **Asynchronous Messaging**: Low-latency event streaming via Kafka handles communication between the orchestrator and microservices, ensuring loose temporal coupling and reliable message delivery.
* **Real-Time Telemetry**: Updates are streamed directly from the engine to the frontend UI via Server-Sent Events (SSE) over HTTP, visualizing step execution and rollbacks dynamically.

## 2. Advanced Functional Programming Concepts Utilized

This project leverages type-safe, declarative functional programming patterns to replace traditional Enterprise Java approaches:

* **Virtual Threading (Fibers)**: Utilizing ZIO's native fiber primitives to spawn lightweight, non-blocking execution contexts per transaction. This allows the system to process thousands of concurrent sagas efficiently without thread pool exhaustion.
* **Atomic State Mutability (Ref)**: Thread-safe tracking of in-flight compensations using concurrent atomic reference variables (ZIO.Ref), eliminating structural mutability issues or explicit synchronization overhead.
* **Monadic Composition**: Deeply nested workflow structures are flattened into readable, declarative linear steps using Scala for-comprehensions, guaranteeing structural determinism and type safety.
* **Declarative Resiliency (Schedule)**: Transient network and infrastructure errors are handled via ZIO's Schedule primitive, providing elegant, single-line retry behaviors including exponential backoff and jitter.

## 3. Technology Stack

### Backend

* **Language**: Scala 3.x
* **Effect System**: ZIO 2.x (Core, Streams, JSON)
* **HTTP/Streaming HTTP**: ZIO-HTTP
* **Message Broker Client**: ZIO-Kafka
* **Database Access**: Quill / JDBC (PostgreSQL Alpine)

### Frontend

* **Framework**: React 18+ (TypeScript, Tailwind CSS)
* **Streaming Client**: EventSource API (Native SSE)

### Infrastructure (Free Developer Tiers)

* **Local Engine**: Podman Compose, Redpanda (C++ drop-in Kafka alternative)
* **Cloud Hosting**: Render / Fly.io (Backend), Vercel (Frontend)
* **Managed Services**: Upstash (Serverless Kafka), Supabase / Neon (Serverless PostgreSQL)

## 4. Project Structure

The codebase is organized as an SBT multi-module monorepo alongside an isolated frontend application:

```text
├── build.sbt                # Multi-module Scala compilation layout
├── compose.yml              # Development infrastructure (Redpanda, PostgreSQL)
├── core-orchestrator/       # ZIO engine, Saga log database management, SSE streaming
│   └── src/main/scala/
├── shared-events/           # Shared transactional event schemas (Scala 3 Enums)
├── mock-services/           # Simulated independent service actors listening to Kafka topics
├── frontend/                # React dashboard for visual transaction tracking
│   ├── src/                 # Event-driven visual timeline components
│   └── package.json
└── README.md
```

## 5. Local Setup and Execution

### Prerequisites

* Podman and Podman Compose
* SBT (Scala Build Tool)
* Node.js and npm (for the tracking dashboard)

### Step 1: Spin Up Local Infrastructure

Launch the lightweight message broker and persistence database:

```bash
        podman-compose up -d
```

### Step 2: Run the Backend Orchestrator

Launch the SBT shell and start the core engine and mock services:

```bash
sbt
> project coreOrchestrator
> run
```

### Step 3: Run the Dashboard UI

In a separate terminal, navigate to the frontend directory and start the local development server:

```bash
cd frontend
npm install
npm run dev
```

Open http://localhost:5173 to view the step-by-step transaction tracker.
