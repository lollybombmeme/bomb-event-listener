# Smart Contract Subscriber

## Overview

The Smart Contract Subscriber service monitors events emitted from a smart contract using an RPC URL. It continuously listens for events and communicates with an API service to produce bridge messages based on these events. This service facilitates real-time integration between blockchain smart contracts and external APIs.

## Features

-   Subscribe to events emitted from a smart contract
-   Receive event notifications via RPC URL
-   Transform received events into bridge messages
-   Communicate with an API service to send bridge messages

## Installation

To get started with the Smart Contract Subscriber service, follow these steps:
Requirement: Node: v20.x, Docker compose

1. **Install dependencies:**

    ```bash
    npm install
    ```

2. **Set up environment variables:**
   Create a `.env` file in the root directory and add the necessary environment variables:
3. **Run the service:**
    ```bash
    docker compose up -d --build
    ```

## Usage

Once the service is running, it will start listening for events from the specified smart contract via the RPC URL. It will then transform these events into bridge messages and send them to the API service.
