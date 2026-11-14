flowchart TD

%% MAIN FLOW
A["Raw Multimodal Data"] --> B["Data Ingestion"] --> C["Normalization Process"] --> D["Normalized Dataset"]

%% INGESTION DETAILS
subgraph I["Data Ingestion"]
    direction TD
    B1["API Retrieval"]
    B2["Streaming / Snapshot Updates"]
    B3["Raw Record Capture"]
end

B --> B1
B1 --> B2 --> B3

%% NORMALIZATION DETAILS
subgraph N["Normalization Process"]
    direction TD

    subgraph NS["Source-Specific Processing"]
        direction TD
        C1["Format Handling"]
        C2["Identifier Stabilization"]
        C3["Value Standardization"]
        C4["Preserve Source Semantics"]
    end

    subgraph SA["Schema Alignment"]
        direction TD
        D1["Map to Common Fields"]
        D2["(source, timestamp, value, identifiers)"]
    end

    subgraph TP["Temporal Standardization"]
        direction TD
        E1["UTC Alignment"]
        E2["Minute-level Harmonization"]
    end

end

C --> C1
C4 --> D1
D2 --> E1
E2 --> D

%% INTERNAL FLOWS
C1 --> C2 --> C3 --> C4
D1 --> D2
E1 --> E2
