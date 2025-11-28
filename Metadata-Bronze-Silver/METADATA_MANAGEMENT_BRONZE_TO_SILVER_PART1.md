# Metadata Management in Bronze to Silver ETL Pipeline - Part 1

**Patent Documentation: Big Data Metadata Processing & Storage**

---

## 📋 Table of Contents

1. [Executive Summary](#executive-summary)
2. [Metadata Architecture Overview](#metadata-architecture-overview)
3. [Apache Atlas Technology Stack](#apache-atlas-technology-stack)
4. [Metadata Lifecycle in ETL Pipeline](#metadata-lifecycle-in-etl-pipeline)
5. [Metadata Capture & Registration](#metadata-capture--registration)
6. [Metadata Storage in Atlas](#metadata-storage-in-atlas)
7. [Metadata Enrichment Process](#metadata-enrichment-process)
8. [Lineage Tracking Implementation](#lineage-tracking-implementation)
9. [Search & Discovery (Solr)](#search--discovery-solr)
10. [Performance & Scalability](#performance--scalability)

---

## 🎯 Executive Summary

### Innovation Overview

Portal INSIGHTERA mengimplementasikan **automated metadata management system** yang terintegrasi dengan **Apache Atlas** untuk tracking, governance, dan lineage dari data yang mengalir melalui Bronze to Silver ETL pipeline.

**Key Innovations**:
1. ✅ **Automatic Metadata Capture** - Metadata ter-register otomatis saat file upload
2. ✅ **Real-time Lineage Tracking** - Track transformasi Bronze → Silver dengan detail lengkap
3. ✅ **Multi-layer Storage** - Atlas (HBase) + Solr + Zookeeper untuk high availability
4. ✅ **Bidirectional Integration** - Portal backend ↔ Spark ETL ↔ Atlas
5. ✅ **Rich Metadata Model** - Technical, business, operational metadata terintegrasi

### Technology Stack

```
┌─────────────────────────────────────────────────────────────────┐
│  APACHE ATLAS - Metadata & Governance Platform                  │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ │
│                                                                  │
│  Component Stack:                                                │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐               │
│  │   SOLR     │  │   HBASE    │  │ ZOOKEEPER  │               │
│  │ (Search &  │  │ (Metadata  │  │(Coordination│               │
│  │  Indexing) │  │  Storage)  │  │  Service)  │               │
│  └────────────┘  └────────────┘  └────────────┘               │
│                                                                  │
│  Atlas REST API (Port 21000)                                    │
│  - Entity Management                                            │
│  - Type Definitions                                             │
│  - Lineage Tracking                                             │
│  - Search & Discovery                                           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
           │                                │
           │                                │
           ▼                                ▼
┌──────────────────────┐         ┌──────────────────────┐
│  PORTAL BACKEND      │         │  SPARK ETL JOBS      │
│  (Node.js)           │         │  (PySpark)           │
│                      │         │                      │
│  - atlas.service.js  │         │  - Metadata writer   │
│  - Auto-registration │         │  - Lineage tracker   │
│  - GUID management   │         │  - Quality metrics   │
└──────────────────────┘         └──────────────────────┘
```

---

## 🏗️ Metadata Architecture Overview

### Metadata Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│  STAGE 1: FILE UPLOAD & INITIAL REGISTRATION                    │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ │
│                                                                  │
│  User uploads file → Portal Backend                             │
│                                                                  │
│  1️⃣ File saved to ADLS Bronze container                        │
│     Path: bronze/akademik/123/mahasiswa.csv                     │
│                                                                  │
│  2️⃣ Create record in PostgreSQL database                       │
│     Table: uploads                                              │
│     Columns: id, fileName, filePath, uploadedBy, ...            │
│                                                                  │
│  3️⃣ AUTOMATIC ATLAS REGISTRATION                               │
│     ┌─────────────────────────────────────────────────┐        │
│     │ atlas.service.createADLSEntity()                │        │
│     │                                                 │        │
│     │ POST /api/atlas/v2/entity                       │        │
│     │ {                                               │        │
│     │   "entity": {                                   │        │
│     │     "typeName": "adls_gen2_resource",          │        │
│     │     "attributes": {                             │        │
│     │       "qualifiedName": "bronze://...",         │        │
│     │       "name": "mahasiswa.csv",                 │        │
│     │       "owner": "staff@univ.ac.id",             │        │
│     │       "storageAccount": "insighteradl",        │        │
│     │       "container": "bronze",                   │        │
│     │       "path": "akademik/123/mahasiswa.csv",    │        │
│     │       "fileType": "csv",                       │        │
│     │       "sizeBytes": 2048576,                    │        │
│     │       "recordCount": 1500,                     │        │
│     │       "dataLayer": "BRONZE",                   │        │
│     │       "uploadDate": 1732780800000              │        │
│     │     }                                           │        │
│     │   }                                             │        │
│     │ }                                               │        │
│     └─────────────────────────────────────────────────┘        │
│                                                                  │
│  4️⃣ Atlas returns GUID                                         │
│     Example: "a1b2c3d4-e5f6-7890-abcd-ef1234567890"            │
│                                                                  │
│  5️⃣ Update PostgreSQL with Atlas GUID                          │
│     UPDATE uploads SET atlasGuid = 'a1b2c3d4...' WHERE id = 123│
│                                                                  │
│  6️⃣ Add classification tag to entity                           │
│     atlas.service.addClassifications(guid, ['Bronze'])          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  STAGE 2: STAGING ANALYSIS & METADATA ENRICHMENT                │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ │
│                                                                  │
│  Staging process (WOA + EDA) generates additional metadata:     │
│                                                                  │
│  - Data quality score: 95.5%                                    │
│  - Column statistics: min, max, mean, stddev, null_count        │
│  - Data profiling results: patterns, outliers                   │
│  - Schema inference: detected types, constraints                │
│                                                                  │
│  ⚠️ At this stage, metadata NOT yet updated in Atlas           │
│  (Will be updated after Silver transformation)                  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  STAGE 3: ETL PIPELINE TRIGGER & LINEAGE PREPARATION            │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ │
│                                                                  │
│  User clicks "Promote to Silver"                                │
│                                                                  │
│  1️⃣ Portal Backend triggers Airflow DAG                        │
│     POST /api/v1/dags/bronze_to_silver_spark_pipeline/dagRuns   │
│                                                                  │
│  2️⃣ DAG configuration includes Atlas metadata                  │
│     {                                                            │
│       "upload_id": 123,                                          │
│       "bronze_atlas_guid": "a1b2c3d4-...",  ← Atlas GUID       │
│       "bronze_path": "abfss://bronze@.../mahasiswa.csv",        │
│       "data_mart_code": "akademik",                             │
│       "quality_score": 95.5                                      │
│     }                                                            │
│                                                                  │
│  3️⃣ Airflow DAG starts execution                               │
│     Task 1: prepare_spark_config                                │
│     - Fetch Bronze entity metadata from Atlas                   │
│     - Prepare Silver output paths                               │
│     - Pass Atlas GUID to Spark job                              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  STAGE 4: SPARK ETL EXECUTION & METADATA CREATION               │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ │
│                                                                  │
│  PySpark job: bronze_to_silver_transformation.py                │
│                                                                  │
│  1️⃣ Read Bronze data (Extract phase)                           │
│     df = spark.read.csv(bronze_path)                            │
│     original_count = df.count()                                 │
│                                                                  │
│  2️⃣ Apply transformations (Transform phase)                    │
│     - Standardize column names                                  │
│     - Remove duplicates                                         │
│     - Handle missing values                                     │
│     - Convert data types                                        │
│     - Add metadata columns                                      │
│     - Apply quality rules                                       │
│                                                                  │
│  3️⃣ COLLECT PROCESSING METADATA                                │
│     ┌─────────────────────────────────────────────────┐        │
│     │ transformation_metadata = {                     │        │
│     │   "original_count": 1500,                       │        │
│     │   "duplicates_removed": 10,                     │        │
│     │   "nulls_filled": 25,                           │        │
│     │   "type_conversions": 5,                        │        │
│     │   "final_count": 1490,                          │        │
│     │   "quality_score": 98.3,                        │        │
│     │   "processing_time_seconds": 45.2,              │        │
│     │   "columns_standardized": 15,                   │        │
│     │   "quality_rules_applied": 8,                   │        │
│     │   "quality_rules_passed": 1465,                 │        │
│     │   "quality_rules_failed": 25                    │        │
│     │ }                                                │        │
│     └─────────────────────────────────────────────────┘        │
│                                                                  │
│  4️⃣ Write to Silver layer (Load phase)                         │
│     silver_path = "silver/akademik/2025/11/28/upload_123/"      │
│     df.write.partitionBy(...).parquet(silver_path)              │
│                                                                  │
│  5️⃣ REGISTER SILVER ENTITY IN ATLAS                            │
│     (Via Atlas REST API from Spark job)                         │
│     ┌─────────────────────────────────────────────────┐        │
│     │ POST /api/atlas/v2/entity                       │        │
│     │ {                                               │        │
│     │   "entity": {                                   │        │
│     │     "typeName": "adls_gen2_resource",          │        │
│     │     "attributes": {                             │        │
│     │       "qualifiedName": "silver://...",         │        │
│     │       "name": "mahasiswa_silver",              │        │
│     │       "owner": "etl_system",                   │        │
│     │       "container": "silver",                   │        │
│     │       "path": "akademik/2025/11/28/...",       │        │
│     │       "fileType": "parquet",                   │        │
│     │       "recordCount": 1490,                     │        │
│     │       "dataLayer": "SILVER",                   │        │
│     │       "processingDate": 1732780800000,         │        │
│     │       "qualityScore": 98.3,                    │        │
│     │       "sourceUploadId": 123                    │        │
│     │     }                                           │        │
│     │   }                                             │        │
│     │ }                                               │        │
│     └─────────────────────────────────────────────────┘        │
│                                                                  │
│  6️⃣ CREATE ETL PROCESS ENTITY (Lineage)                        │
│     ┌─────────────────────────────────────────────────┐        │
│     │ POST /api/atlas/v2/entity                       │        │
│     │ {                                               │        │
│     │   "entity": {                                   │        │
│     │     "typeName": "etl_process",                 │        │
│     │     "attributes": {                             │        │
│     │       "qualifiedName": "bronze_to_silver_123", │        │
│     │       "name": "Bronze→Silver ETL",             │        │
│     │       "inputs": [                               │        │
│     │         {"guid": "a1b2c3d4-..."}  ← Bronze     │        │
│     │       ],                                        │        │
│     │       "outputs": [                              │        │
│     │         {"guid": "f9e8d7c6-..."}  ← Silver     │        │
│     │       ],                                        │        │
│     │       "processType": "SPARK_ETL",              │        │
│     │       "transformations": {                      │        │
│     │         "duplicates_removed": 10,              │        │
│     │         "nulls_filled": 25,                    │        │
│     │         "type_conversions": 5                  │        │
│     │       },                                        │        │
│     │       "executionTime": 45.2,                   │        │
│     │       "executedBy": "spark_user",              │        │
│     │       "executionDate": 1732780800000           │        │
│     │     }                                           │        │
│     │   }                                             │        │
│     │ }                                               │        │
│     └─────────────────────────────────────────────────┘        │
│                                                                  │
│  7️⃣ Add Silver classification                                  │
│     atlas.addClassifications(silver_guid, ['Silver'])           │
│                                                                  │
│  8️⃣ Return metadata to Airflow                                 │
│     {                                                            │
│       "status": "SUCCESS",                                       │
│       "silver_atlas_guid": "f9e8d7c6-...",                      │
│       "etl_process_guid": "b2c3d4e5-...",                       │
│       "transformation_metadata": {...}                           │
│     }                                                            │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  STAGE 5: WEBHOOK CALLBACK & DATABASE UPDATE                    │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ │
│                                                                  │
│  Airflow calls Portal webhook:                                  │
│  POST /api/etl/webhook/complete                                 │
│                                                                  │
│  Payload:                                                        │
│  {                                                               │
│    "uploadId": 123,                                              │
│    "status": "SUCCESS",                                          │
│    "silverLayerPath": "silver/akademik/2025/11/28/...",         │
│    "silverAtlasGuid": "f9e8d7c6-...",                           │
│    "etlProcessGuid": "b2c3d4e5-...",                            │
│    "recordsProcessed": 1490,                                     │
│    "transformationsApplied": {...}                               │
│  }                                                               │
│                                                                  │
│  Portal Backend updates database:                               │
│  UPDATE uploads SET                                              │
│    silverLayerPath = 'silver/akademik/...',                     │
│    silverAtlasGuid = 'f9e8d7c6-...',                            │
│    uploadStatus = 'VALIDATED',                                  │
│    etlCompletedAt = NOW()                                        │
│  WHERE id = 123;                                                 │
│                                                                  │
│  INSERT INTO etl_jobs (                                          │
│    uploadId, dagRunId, jobStatus,                               │
│    bronzeAtlasGuid, silverAtlasGuid, etlProcessGuid,            │
│    transformationMetrics, ...                                    │
│  ) VALUES (...);                                                 │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Metadata Categories

| Category | Description | Storage Location | Examples |
|----------|-------------|------------------|----------|
| **Technical Metadata** | Schema, data types, file formats | Atlas (HBase) | Column names, types, nullability |
| **Operational Metadata** | Processing stats, timestamps | Atlas + PostgreSQL | Record counts, processing time |
| **Business Metadata** | Ownership, descriptions | Atlas | Owner name, data mart, purpose |
| **Quality Metadata** | Validation results, scores | Atlas + PostgreSQL | Quality score, rules passed/failed |
| **Lineage Metadata** | Data flow, transformations | Atlas (graph) | Bronze → ETL Process → Silver |

---

## 🔧 Apache Atlas Technology Stack

### Component Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  APACHE ATLAS ARCHITECTURE                                      │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  ATLAS REST API (Port 21000)                             │  │
│  │  - Entity CRUD operations                                │  │
│  │  - Type management                                       │  │
│  │  - Search & Discovery                                    │  │
│  │  - Lineage queries                                       │  │
│  └─────────────┬────────────────────────────────────────────┘  │
│                │                                                 │
│  ┌─────────────▼────────────────────────────────────────────┐  │
│  │  ATLAS CORE                                              │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │  │
│  │  │ Type System  │  │Graph Engine  │  │ Notification │  │  │
│  │  │ - EntityDefs │  │ - Vertices    │  │ - Kafka/Hook │  │  │
│  │  │ - ClassDefs  │  │ - Edges       │  │ - Listeners  │  │  │
│  │  │ - RelDefs    │  │ - Properties  │  │              │  │  │
│  │  └──────────────┘  └──────────────┘  └──────────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                │                      │                         │
│  ┌─────────────▼─────────┐  ┌────────▼──────────┐             │
│  │  APACHE HBASE         │  │  APACHE SOLR      │             │
│  │  (Metadata Storage)   │  │  (Search Index)   │             │
│  │  ━━━━━━━━━━━━━━━━━━ │  │  ━━━━━━━━━━━━━━━ │             │
│  │                       │  │                   │             │
│  │  Tables:              │  │  Collections:     │             │
│  │  - atlas_entity       │  │  - vertex_index   │             │
│  │  - atlas_relationship │  │  - edge_index     │             │
│  │  - atlas_audit        │  │  - fulltext_index │             │
│  │                       │  │                   │             │
│  │  Stores:              │  │  Features:        │             │
│  │  - Entity attributes  │  │  - Full-text      │             │
│  │  - Relationships      │  │  - Faceted search │             │
│  │  - Audit logs         │  │  - Wildcards      │             │
│  │  - Classifications    │  │  - Fuzzy matching │             │
│  │                       │  │                   │             │
│  └───────────┬───────────┘  └───────────────────┘             │
│              │                                                  │
│  ┌───────────▼──────────────────────────────────────────────┐  │
│  │  APACHE ZOOKEEPER                                        │  │
│  │  (Coordination Service)                                  │  │
│  │  ━━━━━━━━━━━━━━━━━━━                                   │  │
│  │                                                           │  │
│  │  Manages:                                                │  │
│  │  - HBase region servers                                  │  │
│  │  - Solr cloud nodes                                      │  │
│  │  - Leader election                                       │  │
│  │  - Configuration management                              │  │
│  │  - Service discovery                                     │  │
│  │                                                           │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Technology Roles

#### 1. **Apache HBase** - Primary Metadata Storage

**Purpose**: Store entity metadata, relationships, and audit logs

**Schema Design**:
```
Table: atlas_entity
━━━━━━━━━━━━━━━━━
Row Key: <entity_guid>
Column Families:
  - cf_entity: Entity attributes
  - cf_metadata: System metadata (created, updated, version)
  - cf_classifications: Applied tags/classifications

Example Row:
Row: a1b2c3d4-e5f6-7890-abcd-ef1234567890
  cf_entity:
    typeName: "adls_gen2_resource"
    qualifiedName: "bronze://akademik/123/mahasiswa.csv"
    name: "mahasiswa.csv"
    owner: "staff@univ.ac.id"
    storageAccount: "insighteradl"
    container: "bronze"
    path: "akademik/123/mahasiswa.csv"
    fileType: "csv"
    sizeBytes: 2048576
    recordCount: 1500
    dataLayer: "BRONZE"
  cf_metadata:
    createdBy: "atlas_admin"
    createTime: 1732780800000
    modifiedBy: "atlas_admin"
    modifyTime: 1732780800000
    version: 1
  cf_classifications:
    Bronze: {"typeName": "Bronze"}
```

**Why HBase?**
- ✅ **Horizontal scalability** - Handle millions of entities
- ✅ **Fast random access** - Get entity by GUID in milliseconds
- ✅ **Column-oriented** - Efficient storage for sparse attributes
- ✅ **Versioning** - Track metadata changes over time
- ✅ **Strong consistency** - ACID guarantees via Zookeeper

#### 2. **Apache Solr** - Search & Indexing

**Purpose**: Enable fast full-text search and faceted queries

**Collections**:
```
Collection: vertex_index
━━━━━━━━━━━━━━━━━━━━━━
Documents indexed from HBase entities

Example Document:
{
  "guid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "typeName": "adls_gen2_resource",
  "qualifiedName": "bronze://akademik/123/mahasiswa.csv",
  "name": "mahasiswa.csv",
  "owner": "staff@univ.ac.id",
  "dataLayer": "BRONZE",
  "container": "bronze",
  "classifications": ["Bronze"],
  "createTime": "2025-11-28T10:00:00Z",
  "modifyTime": "2025-11-28T10:00:00Z",
  
  // Full-text indexed fields
  "text": "mahasiswa.csv staff@univ.ac.id bronze akademik",
  
  // Facets for filtering
  "dataLayer_s": "BRONZE",
  "typeName_s": "adls_gen2_resource",
  "owner_s": "staff@univ.ac.id"
}
```

**Search Capabilities**:
```
# Find all Bronze layer files
q=dataLayer_s:BRONZE

# Find files by owner
q=owner_s:"staff@univ.ac.id"

# Full-text search
q=text:mahasiswa

# Faceted search (count by type)
q=*:*&facet=true&facet.field=typeName_s

# Wildcard search
q=name:mahasiswa*

# Date range
q=createTime:[2025-11-01T00:00:00Z TO 2025-11-30T23:59:59Z]
```

**Why Solr?**
- ✅ **Fast full-text search** - Inverted index for text fields
- ✅ **Faceted navigation** - Group by type, owner, layer
- ✅ **Wildcard/fuzzy** - Flexible search patterns
- ✅ **Real-time indexing** - Changes appear in seconds
- ✅ **Distributed** - SolrCloud for high availability

#### 3. **Apache Zookeeper** - Coordination Service

**Purpose**: Manage distributed components and ensure consistency

**Responsibilities**:

1. **HBase Coordination**:
   - Track active RegionServers
   - Manage region assignments
   - Handle failover/recovery

2. **Solr Cloud Coordination**:
   - Leader election for shards
   - Cluster state management
   - Configuration distribution

3. **Atlas High Availability**:
   - Active/passive Atlas instances
   - Automatic failover
   - Session management

**Why Zookeeper?**
- ✅ **Leader election** - Ensure single writer
- ✅ **Configuration sync** - Consistent cluster state
- ✅ **Service discovery** - Dynamic node registration
- ✅ **Locks & barriers** - Distributed coordination
- ✅ **Fault tolerance** - Quorum-based consensus

---

## 🔄 Metadata Lifecycle in ETL Pipeline

### Phase-by-Phase Metadata Processing

```
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 1: BRONZE LAYER - Initial Metadata Capture               │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ │
│                                                                  │
│  Trigger: File upload to Portal                                 │
│  Location: Portal Backend (Node.js)                             │
│  Service: atlas.service.js                                      │
│                                                                  │
│  Metadata Captured:                                              │
│  ┌────────────────────────────────────────────────────┐         │
│  │ TECHNICAL METADATA                                 │         │
│  │ - qualifiedName: Unique identifier                │         │
│  │ - name: Human-readable file name                  │         │
│  │ - fileType: csv | parquet | json | xlsx           │         │
│  │ - sizeBytes: File size in bytes                   │         │
│  │ - recordCount: Estimated row count                │         │
│  │ - storageAccount: ADLS account name               │         │
│  │ - container: bronze                               │         │
│  │ - path: Full path in container                    │         │
│  │                                                    │         │
│  │ BUSINESS METADATA                                 │         │
│  │ - owner: User who uploaded                        │         │
│  │ - description: User-provided description          │         │
│  │ - dataLayer: BRONZE                               │         │
│  │ - dataMart: akademik | keuangan | ...            │         │
│  │                                                    │         │
│  │ OPERATIONAL METADATA                              │         │
│  │ - uploadDate: Timestamp (epoch millis)            │         │
│  │ - uploadedBy: Email of uploader                   │         │
│  │ - sourceUploadId: Link to PostgreSQL uploads      │         │
│  └────────────────────────────────────────────────────┘         │
│                                                                  │
│  Storage:                                                        │
│  - HBase: Full entity attributes                                │
│  - Solr: Indexed for search                                     │
│  - PostgreSQL: uploads.atlasGuid = <guid>                       │
│                                                                  │
│  Classifications Applied:                                        │
│  - "Bronze" (data layer tag)                                    │
│  - Optional: "PII", "Sensitive" (based on content)              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 2: STAGING ANALYSIS - Metadata Enrichment                │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ │
│                                                                  │
│  Trigger: Staging analysis (WOA + EDA)                          │
│  Location: Portal Backend                                       │
│  Storage: PostgreSQL only (not yet in Atlas)                    │
│                                                                  │
│  Additional Metadata Generated:                                  │
│  ┌────────────────────────────────────────────────────┐         │
│  │ DATA QUALITY METRICS                               │         │
│  │ - stagingScore: Overall quality (0-100)           │         │
│  │ - completenessScore: % non-null values            │         │
│  │ - uniquenessScore: % unique values                │         │
│  │ - validityScore: % valid formats                  │         │
│  │ - consistencyScore: % consistent values           │         │
│  │                                                    │         │
│  │ COLUMN PROFILING                                  │         │
│  │ For each column:                                  │         │
│  │ - dataType: Inferred type                         │         │
│  │ - nullCount: # of nulls                           │         │
│  │ - distinctCount: # of unique values               │         │
│  │ - min / max: Range (numeric/date)                │         │
│  │ - mean / stddev: Statistics (numeric)            │         │
│  │ - topValues: Most frequent values (string)        │         │
│  │ - patterns: Detected patterns (regex)             │         │
│  │ - outliers: Anomalous values                      │         │
│  └────────────────────────────────────────────────────┘         │
│                                                                  │
│  ⚠️ Note: This metadata stored in PostgreSQL staging_results   │
│  table. Will be sent to Atlas during ETL execution.             │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 3: ETL EXECUTION - Transformation Metadata               │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ │
│                                                                  │
│  Trigger: Airflow DAG execution                                 │
│  Location: Spark job (bronze_to_silver_transformation.py)       │
│  Service: Atlas REST API calls from PySpark                     │
│                                                                  │
│  Step 1: Read Bronze metadata                                   │
│  ┌────────────────────────────────────────────────────┐         │
│  │ # In Spark job initialization                      │         │
│  │ bronze_guid = config['bronze_atlas_guid']          │         │
│  │                                                     │         │
│  │ # Fetch Bronze entity from Atlas                   │         │
│  │ bronze_entity = atlas_client.get_entity(bronze_guid│         │
│  │                                                     │         │
│  │ # Extract metadata                                 │         │
│  │ bronze_qualified_name = bronze_entity['qualifiedName'│        │
│  │ bronze_owner = bronze_entity['owner']              │         │
│  │ bronze_path = bronze_entity['path']                │         │
│  └────────────────────────────────────────────────────┘         │
│                                                                  │
│  Step 2: Collect transformation metrics                         │
│  ┌────────────────────────────────────────────────────┐         │
│  │ transformation_metadata = {                        │         │
│  │   "original_count": 1500,                          │         │
│  │   "duplicates_removed": 10,                        │         │
│  │   "nulls_filled": 25,                              │         │
│  │   "type_conversions": 5,                           │         │
│  │   "final_count": 1490,                             │         │
│  │   "quality_score": 98.3,                           │         │
│  │   "processing_time_seconds": 45.2,                 │         │
│  │   "columns_before": 18,                            │         │
│  │   "columns_after": 24, # Added metadata cols       │         │
│  │   "transformations_applied": [                     │         │
│  │     "standardize_column_names",                    │         │
│  │     "remove_duplicates",                           │         │
│  │     "handle_missing_values",                       │         │
│  │     "convert_data_types",                          │         │
│  │     "add_metadata_columns",                        │         │
│  │     "apply_quality_rules"                          │         │
│  │   ]                                                 │         │
│  │ }                                                   │         │
│  └────────────────────────────────────────────────────┘         │
│                                                                  │
│  Step 3: Register Silver entity                                 │
│  ┌────────────────────────────────────────────────────┐         │
│  │ silver_entity = {                                  │         │
│  │   "typeName": "adls_gen2_resource",               │         │
│  │   "attributes": {                                  │         │
│  │     "qualifiedName": "silver://akademik/...",     │         │
│  │     "name": "mahasiswa_silver",                   │         │
│  │     "owner": bronze_owner, # Inherit from Bronze  │         │
│  │     "container": "silver",                        │         │
│  │     "path": "akademik/2025/11/28/upload_123/",    │         │
│  │     "fileType": "parquet",                        │         │
│  │     "recordCount": 1490,                          │         │
│  │     "dataLayer": "SILVER",                        │         │
│  │     "processingDate": current_timestamp(),        │         │
│  │     "qualityScore": 98.3,                         │         │
│  │     "sourceUploadId": 123,                        │         │
│  │     "transformationMetrics": json.dumps(          │         │
│  │       transformation_metadata                     │         │
│  │     )                                              │         │
│  │   }                                                │         │
│  │ }                                                  │         │
│  │                                                    │         │
│  │ # POST to Atlas                                    │         │
│  │ response = atlas_client.create_entity(silver_entity│         │
│  │ silver_guid = response['guidAssignments'][...]     │         │
│  └────────────────────────────────────────────────────┘         │
│                                                                  │
│  Step 4: Create ETL Process entity (Lineage)                    │
│  ┌────────────────────────────────────────────────────┐         │
│  │ etl_process = {                                    │         │
│  │   "typeName": "etl_process",                      │         │
│  │   "attributes": {                                  │         │
│  │     "qualifiedName": "bronze_to_silver_upload_123"│         │
│  │     "name": "Bronze→Silver: mahasiswa.csv",       │         │
│  │     "processType": "SPARK_ETL",                   │         │
│  │     "inputs": [                                    │         │
│  │       {"guid": bronze_guid}                       │         │
│  │     ],                                             │         │
│  │     "outputs": [                                   │         │
│  │       {"guid": silver_guid}                       │         │
│  │     ],                                             │         │
│  │     "transformations": transformation_metadata,    │         │
│  │     "executionTime": 45.2,                        │         │
│  │     "executedBy": "spark_etl_user",               │         │
│  │     "executionDate": current_timestamp(),         │         │
│  │     "dagId": "bronze_to_silver_spark_pipeline",   │         │
│  │     "dagRunId": "manual__2025-11-28T10:00:00",    │         │
│  │     "sparkAppId": "app-20251128100000-0001"       │         │
│  │   }                                                │         │
│  │ }                                                  │         │
│  │                                                    │         │
│  │ # POST to Atlas                                    │         │
│  │ response = atlas_client.create_entity(etl_process) │         │
│  │ etl_process_guid = response['guidAssignments'][...│         │
│  └────────────────────────────────────────────────────┘         │
│                                                                  │
│  Step 5: Add classifications                                    │
│  ┌────────────────────────────────────────────────────┐         │
│  │ # Add Silver classification                        │         │
│  │ atlas_client.add_classifications(                  │         │
│  │   silver_guid,                                     │         │
│  │   ['Silver']                                       │         │
│  │ )                                                  │         │
│  │                                                    │         │
│  │ # Copy classifications from Bronze                │         │
│  │ if bronze_entity.has('PII'):                       │         │
│  │   atlas_client.add_classifications(               │         │
│  │     silver_guid,                                   │         │
│  │     ['PII']                                        │         │
│  │   )                                                │         │
│  └────────────────────────────────────────────────────┘         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 4: POST-ETL - Database Synchronization                   │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ │
│                                                                  │
│  Trigger: Airflow webhook callback                              │
│  Location: Portal Backend                                       │
│  Service: ETL webhook controller                                │
│                                                                  │
│  Actions:                                                        │
│  1. Update uploads table                                        │
│     UPDATE uploads SET                                           │
│       silverLayerPath = 'silver/akademik/...',                  │
│       silverAtlasGuid = 'f9e8d7c6-...',                         │
│       uploadStatus = 'VALIDATED',                               │
│       recordCount = 1490,                                        │
│       etlCompletedAt = NOW()                                     │
│     WHERE id = 123;                                              │
│                                                                  │
│  2. Create etl_jobs record                                      │
│     INSERT INTO etl_jobs (                                       │
│       uploadId, jobName, jobStatus,                             │
│       dagId, dagRunId,                                           │
│       bronzeAtlasGuid, silverAtlasGuid, etlProcessGuid,         │
│       sourceLayer, targetLayer,                                  │
│       recordsProcessed, transformationMetrics,                   │
│       startedAt, completedAt                                     │
│     ) VALUES (                                                   │
│       123, 'Bronze→Silver: mahasiswa.csv', 'SUCCESS',           │
│       'bronze_to_silver_spark_pipeline', 'manual__...',         │
│       'a1b2c3d4-...', 'f9e8d7c6-...', 'b2c3d4e5-...',          │
│       'BRONZE', 'SILVER',                                        │
│       1490, '{"duplicates_removed": 10, ...}',                  │
│       '2025-11-28 10:00:00', '2025-11-28 10:01:30'             │
│     );                                                           │
│                                                                  │
│  Result:                                                         │
│  - PostgreSQL: Updated with Atlas GUIDs and ETL metrics         │
│  - Atlas HBase: Contains full lineage graph                     │
│  - Atlas Solr: Bronze + Silver entities indexed for search      │
│  - Users can now:                                               │
│    * Search for entities in BrowsePage                          │
│    * View lineage in EntityDetailModal                          │
│    * Query transformations in DataLineagePage                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📝 Metadata Capture & Registration

### Backend Implementation (Node.js)

**File**: `backend/src/controllers/file.controller.js`

```javascript
// After file upload to ADLS Bronze
async function uploadFileToBronze(req, res) {
  const { file, dataMartId, description } = req.body;
  
  try {
    // 1. Upload to ADLS
    const adlsResult = await adlsService.uploadFile(
      file,
      `bronze/${dataMart.code}/${upload.id}/${file.originalname}`
    );
    
    // 2. Create upload record in PostgreSQL
    const upload = await prisma.upload.create({
      data: {
        originalFileName: file.originalname,
        filePath: adlsResult.blobName,
        fileSize: file.size,
        fileType: getFileType(file.originalname),
        uploadedBy: req.user.email,
        dataMartId: dataMartId,
        uploadStatus: 'UPLOADED',
        // atlasGuid will be set after Atlas registration
      }
    });
    
    // 3. AUTO-REGISTER IN APACHE ATLAS
    try {
      console.log('[Upload] Registering entity in Apache Atlas...');
      
      const atlasResponse = await atlasService.createADLSEntity({
        fileName: file.originalname,
        containerName: 'bronze',
        blobPath: adlsResult.blobName,
        ownerName: req.user.name || req.user.email,
        uploadedBy: req.user.email,
        description: description || `File uploaded to ${dataMart.name}`,
        fileSize: file.size,
        fileType: getFileType(file.originalname),
        uploadedAt: new Date().toISOString(),
        dataLayer: 'BRONZE',
        dataMart: dataMart.name,
        recordCount: estimateRecordCount(file), // Optional
      });
      
      // Extract GUID from Atlas response
      const atlasGuid = atlasResponse.guidAssignments?.[
        Object.keys(atlasResponse.guidAssignments)[0]
      ];
      
      if (atlasGuid) {
        // 4. Add Bronze classification
        await atlasService.addClassifications(atlasGuid, ['Bronze']);
        
        // 5. Update upload with Atlas GUID
        await prisma.upload.update({
          where: { id: upload.id },
          data: { atlasGuid },
        });
        
        console.log(`[Upload] Entity registered with GUID: ${atlasGuid}`);
      }
      
    } catch (atlasError) {
      // Don't fail upload if Atlas registration fails
      console.error('[Upload] Atlas registration failed:', atlasError.message);
    }
    
    res.json({
      success: true,
      uploadId: upload.id,
      atlasGuid: upload.atlasGuid,
    });
    
  } catch (error) {
    console.error('[Upload] Failed:', error);
    res.status(500).json({ error: error.message });
  }
}
```

### Atlas Service Implementation

**File**: `backend/src/services/atlas.service.js`

```javascript
/**
 * Create ADLS Gen2 resource entity in Atlas
 */
async function createADLSEntity(fileData) {
  // Build qualifiedName (unique identifier)
  const qualifiedName = `adls://${fileData.containerName}/${fileData.blobPath}@insighteralake`;
  
  const attributes = {
    qualifiedName,
    name: fileData.fileName,
    storageAccount: 'insighteralake',
    container: fileData.containerName,
    path: fileData.blobPath,
    fileType: fileData.fileType,
    sizeBytes: fileData.fileSize,
    recordCount: fileData.recordCount || 0,
    uploadedBy: fileData.uploadedBy || fileData.ownerName,
    uploadDate: fileData.uploadedAt ? 
      new Date(fileData.uploadedAt).getTime() : Date.now(),
    dataLayer: fileData.dataLayer || 'BRONZE',
    owner: fileData.ownerName,
    description: fileData.description || '',
  };
  
  // Create entity via Atlas REST API
  return await createEntity('adls_gen2_resource', attributes);
}

/**
 * Generic create entity function
 */
async function createEntity(typeName, attributes) {
  const entity = {
    entity: {
      typeName,
      attributes,
      status: 'ACTIVE',
    },
  };
  
  console.log(`Creating Atlas entity: ${typeName}`);
  console.log('Attributes:', JSON.stringify(attributes, null, 2));
  
  const response = await atlasClient.post('/api/atlas/v2/entity', entity);
  
  console.log(`Entity created: ${typeName}`);
  console.log('GUIDs:', response.data.guidAssignments);
  
  return response.data;
}

/**
 * Add classifications (tags) to entity
 */
async function addClassifications(guid, classifications) {
  const classificationsPayload = classifications.map((name) => ({
    typeName: name,
  }));
  
  await atlasClient.post(
    `/api/atlas/v2/entity/guid/${guid}/classifications`,
    classificationsPayload
  );
  
  console.log(`Added classifications to ${guid}:`, classifications);
}
```

---

**[Continue to Part 2 for Silver Layer Metadata, Lineage Implementation, and Search/Discovery...]**

---

## 📌 Summary - Part 1

### Key Innovations Documented

1. **Automated Metadata Capture**
   - Zero manual intervention
   - Metadata captured at upload time
   - Atlas GUID stored in PostgreSQL for bidirectional linking

2. **Technology Integration**
   - Apache Atlas REST API
   - HBase for metadata storage
   - Solr for search indexing
   - Zookeeper for coordination

3. **Metadata Categories**
   - Technical: Schema, types, formats
   - Business: Ownership, descriptions
   - Operational: Processing stats, timestamps
   - Quality: Validation results, scores

4. **Patent-worthy Components**
   - Automatic Bronze entity registration
   - GUID-based linking between PostgreSQL and Atlas
   - Classification inheritance (Bronze → Silver)
   - Transformation metadata capture in Spark

### Next Steps

**Part 2 will cover:**
- Silver entity metadata registration from Spark
- ETL Process entity creation (lineage)
- Search & discovery with Solr
- Metadata querying and visualization
- Performance optimization techniques

---

**Document Version**: 1.0  
**Created**: November 28, 2025  
**Author**: Portal INSIGHTERA Team  
**Patent Classification**: Big Data Metadata Management System
