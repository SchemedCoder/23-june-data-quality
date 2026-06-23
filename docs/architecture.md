# Architecture

Source Systems
(customers / orders / payments / products)

        ↓

Bronze Layer
(raw ingestion)

        ↓

Validation Engine
(schema + null + duplicate checks)

        ↓

Business Rule Engine
(domain validations)

        ↓

Silver Layer
(cleaned validated data)

        ↓

Quality Metrics Table

        ↓

Alerts / Dashboard / Monitoring
