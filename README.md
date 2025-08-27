### C1 - System Context
```mermaid
%% C1 — System Context (MS2 + external Kafka + DB)
flowchart LR
    user[("Developer/Operator")]:::person
    subgraph ms2[MS2 Calculator Service]
        app[MS2 App Process<br/>(Python, confluent-kafka, SQLAlchemy)]:::process
        db[(Walrus DB<br/>(PostgreSQL 16))]:::db
    end
    kafka[Apache Kafka<br/>(External Event Bus)]:::external

    user --> ms2
    app -- SQL read/write --> db
    app -- consume calc.request / produce calc.completed --> kafka

classDef person fill:#FEF3C7,stroke:#F59E0B,stroke-width:2,color:#111;
classDef process fill:#DCFCE7,stroke:#16A34A,stroke-width:2;
classDef db fill:#E0F2FE,stroke:#0EA5E9,stroke-width:2;
classDef external fill:#F3F4F6,stroke:#9CA3AF,stroke-width:2;

```mermaid
%% C2 — Container diagram (within MS2)
flowchart LR
    subgraph ms2[MS2 Calculator Service]
        app[MS2 App Process<br/>(Container/Pod/Process)]:::process
        db[(Walrus DB<br/>(PostgreSQL 16))]:::db
    end

    kafka[Apache Kafka<br/>(External)]:::external

    app -- SQL (SQLAlchemy/psycopg2) --> db
    app -- Kafka I/O --> kafka

    %% Notes
    note1{{"Owns tables:<br/>calc_results, outbox_events, inbox_events"}}:::note
    app --- note1

classDef process fill:#DCFCE7,stroke:#16A34A,stroke-width:2;
classDef db fill:#E0F2FE,stroke:#0EA5E9,stroke-width:2;
classDef external fill:#F3F4F6,stroke:#9CA3AF,stroke-width:2;
classDef note fill:#FFF,stroke:#9CA3AF,stroke-dasharray: 5 5;

```mermaid
%% C3 — Components inside MS2 App Process
flowchart TB
    subgraph app[MS2 App Process (Python)]
        consumer[Kafka Consumer Loop]:::comp
        compute[Compute Service (Mock)]:::comp
        repo[Repository / DB Access]:::comp
        outbox[Outbox Dispatcher]:::comp
        config[Config & Schemas]:::comp
    end

    db[(Walrus DB (PostgreSQL))]:::db
    kafka[Apache Kafka (External)]:::external

    consumer --> compute
    consumer --> repo
    repo --> db
    repo --> outbox
    outbox --> kafka

    %% Topics
    kafka -. consumes .-> consumer
    consumer -. produces .-> kafka

classDef comp fill:#ECFEFF,stroke:#0891B2,stroke-width:2;
classDef db fill:#E0F2FE,stroke:#0EA5E9,stroke-width:2;
classDef external fill:#F3F4F6,stroke:#9CA3AF,stroke-width:2;

```mermaid
%% C4 (informal) — Code/module view mapping
flowchart TB
    main[app/main.py]:::code --> consumer[app/consumer.py]:::code
    main --> dispatcher[app/dispatcher.py]:::code
    consumer --> dbmod[app/db.py]:::code
    dispatcher --> dbmod
    consumer --> schemas[app/schemas.py]:::code
    consumer --> config[app/config.py]:::code
    dispatcher --> config
    dbmod --> config

classDef code fill:#FFF,stroke:#6B7280,stroke-width:2;

