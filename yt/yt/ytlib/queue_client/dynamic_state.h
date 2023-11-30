#pragma once

#include "public.h"

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/queue_client/common.h>
#include <yt/yt/client/queue_client/config.h>

#include <yt/yt/client/ypath/public.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

//! A simple typed interface for accessing given state table. All methods are thread-safe.
template <class TRow>
class TTableBase
    : public TRefCounted
{
public:
    using TRowType = TRow;

    TTableBase(NYPath::TYPath path, NApi::IClientPtr client);

    TFuture<std::vector<TRow>> Select(TStringBuf columns = "*", TStringBuf where = "1 = 1") const;

    TFuture<NApi::TTransactionCommitResult> Insert(std::vector<TRow> rows) const;
    TFuture<NApi::TTransactionCommitResult> Delete(std::vector<TRow> keys) const;

private:
    const NYPath::TYPath Path_;
    const NApi::IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

// Keep fields in-sync with the implementations of all related methods in the corresponding cpp file.
struct TQueueTableRow
{
    TCrossClusterReference Ref;
    std::optional<TRowRevision> RowRevision;
    // Even though some fields are nullable by their nature (e.g. revision),
    // outer-level nullopt is interpreted as Null, i.e. missing value.
    std::optional<NHydra::TRevision> Revision;
    std::optional<NObjectClient::EObjectType> ObjectType;
    std::optional<bool> Dynamic;
    std::optional<bool> Sorted;
    std::optional<TQueueAutoTrimConfig> AutoTrimConfig;
    std::optional<THashMap<TString, TQueueStaticExportConfig>> StaticExportConfig;
    std::optional<TString> QueueAgentStage;
    std::optional<NObjectClient::TObjectId> ObjectId;

    std::optional<TError> SynchronizationError;

    static std::vector<TQueueTableRow> ParseRowRange(
        TRange<NTableClient::TUnversionedRow> rows,
        const NTableClient::TNameTablePtr& nameTable);

    static NApi::IUnversionedRowsetPtr InsertRowRange(TRange<TQueueTableRow> rows);
    static NApi::IUnversionedRowsetPtr DeleteRowRange(TRange<TQueueTableRow> keys);

    static std::vector<TString> GetCypressAttributeNames();

    static TQueueTableRow FromAttributeDictionary(
        const TCrossClusterReference& queue,
        std::optional<TRowRevision> rowRevision,
        const NYTree::IAttributeDictionaryPtr& cypressAttributes);

    bool operator==(const TQueueTableRow& rhs) const = default;
};

void Serialize(const TQueueTableRow& row, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TQueueTable
    : public TTableBase<TQueueTableRow>
{
public:
    TQueueTable(NYPath::TYPath root, NApi::IClientPtr client);
};

DEFINE_REFCOUNTED_TYPE(TQueueTable)

////////////////////////////////////////////////////////////////////////////////

// Keep fields in-sync with the implementations of all related methods in the corresponding cpp file.
struct TConsumerTableRow
{
    TCrossClusterReference Ref;
    std::optional<TRowRevision> RowRevision;
    std::optional<NHydra::TRevision> Revision;
    std::optional<NObjectClient::EObjectType> ObjectType;
    std::optional<bool> TreatAsQueueConsumer;
    std::optional<NTableClient::TTableSchema> Schema;
    std::optional<TString> QueueAgentStage;

    std::optional<TError> SynchronizationError;

    static std::vector<TConsumerTableRow> ParseRowRange(
        TRange<NTableClient::TUnversionedRow> rows,
        const NTableClient::TNameTablePtr& nameTable);

    static NApi::IUnversionedRowsetPtr InsertRowRange(TRange<TConsumerTableRow> rows);
    static NApi::IUnversionedRowsetPtr DeleteRowRange(TRange<TConsumerTableRow> keys);

    static std::vector<TString> GetCypressAttributeNames();

    static TConsumerTableRow FromAttributeDictionary(
        const TCrossClusterReference& consumer,
        std::optional<TRowRevision> rowRevision,
        const NYTree::IAttributeDictionaryPtr& cypressAttributes);

    bool operator==(const TConsumerTableRow& rhs) const = default;
};

void Serialize(const TConsumerTableRow& row, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TConsumerTable
    : public TTableBase<TConsumerTableRow>
{
public:
    TConsumerTable(NYPath::TYPath root, NApi::IClientPtr client);
};

DEFINE_REFCOUNTED_TYPE(TConsumerTable)

////////////////////////////////////////////////////////////////////////////////

struct TQueueAgentObjectMappingTableRow
{
    TCrossClusterReference Object;
    TString QueueAgentHost;

    static std::vector<TQueueAgentObjectMappingTableRow> ParseRowRange(
        TRange<NTableClient::TUnversionedRow> rows,
        const NTableClient::TNameTablePtr& nameTable);

    static NApi::IUnversionedRowsetPtr InsertRowRange(TRange<TQueueAgentObjectMappingTableRow> rows);
    static NApi::IUnversionedRowsetPtr DeleteRowRange(TRange<TQueueAgentObjectMappingTableRow> keys);
};

////////////////////////////////////////////////////////////////////////////////

class TQueueAgentObjectMappingTable
    : public TTableBase<TQueueAgentObjectMappingTableRow>
{
public:
    TQueueAgentObjectMappingTable(NYPath::TYPath root, NApi::IClientPtr client);

    static THashMap<TCrossClusterReference, TString> ToMapping(const std::vector<TQueueAgentObjectMappingTableRow>& rows);
};

DEFINE_REFCOUNTED_TYPE(TQueueAgentObjectMappingTable)

////////////////////////////////////////////////////////////////////////////////

struct TConsumerRegistrationTableRow
{
    TCrossClusterReference Queue;
    TCrossClusterReference Consumer;
    //! If true, this consumer will be considered in automatic trimming performed by queue agents for this queue.
    bool Vital;

    //! Can be set to indicate the fact that this consumer will only be reading the specified queue partitions.
    //! If null, all partitions are assumed to be read.
    std::optional<std::vector<int>> Partitions;

    static std::vector<TConsumerRegistrationTableRow> ParseRowRange(
        TRange<NTableClient::TUnversionedRow> rows,
        const NTableClient::TNameTablePtr& nameTable);

    static NApi::IUnversionedRowsetPtr InsertRowRange(TRange<TConsumerRegistrationTableRow> rows);
    static NApi::IUnversionedRowsetPtr DeleteRowRange(TRange<TConsumerRegistrationTableRow> keys);
};

////////////////////////////////////////////////////////////////////////////////

class TConsumerRegistrationTable
    : public TTableBase<TConsumerRegistrationTableRow>
{
public:
    // NB: The constructor takes the full path, instead of the root path.
    // The registration table can be located on a remote cluster, which should be handled by passing the correct client.
    TConsumerRegistrationTable(NYPath::TYPath path, NApi::IClientPtr client);
};

DEFINE_REFCOUNTED_TYPE(TConsumerRegistrationTable)

////////////////////////////////////////////////////////////////////////////////

class TReplicaInfo
    : public NYTree::TYsonStruct
{
public:
    TString ClusterName;
    NYPath::TYPath ReplicaPath;
    NTabletClient::ETableReplicaState State;
    NTabletClient::ETableReplicaMode Mode;

    REGISTER_YSON_STRUCT(TReplicaInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicaInfo)

class TChaosReplicaInfo
    : public TReplicaInfo
{
public:
    NTabletClient::ETableReplicaContentType ContentType;

    REGISTER_YSON_STRUCT(TChaosReplicaInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosReplicaInfo)

class TReplicatedTableMeta
    : public NYTree::TYsonStruct
{
public:
    THashMap<NTabletClient::TTableReplicaId, TReplicaInfoPtr> Replicas;

    REGISTER_YSON_STRUCT(TReplicatedTableMeta);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicatedTableMeta)

class TChaosReplicatedTableMeta
    : public NYTree::TYsonStruct
{
public:
    NChaosClient::TReplicationCardId ReplicationCardId;
    THashMap<NChaosClient::TReplicaId, TChaosReplicaInfoPtr> Replicas;

    REGISTER_YSON_STRUCT(TChaosReplicatedTableMeta);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosReplicatedTableMeta)

class TGenericReplicatedTableMeta
    : public NYTree::TYsonStruct
{
public:
    TReplicatedTableMetaPtr ReplicatedTableMeta;
    TChaosReplicatedTableMetaPtr ChaosReplicatedTableMeta;

    REGISTER_YSON_STRUCT(TGenericReplicatedTableMeta);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGenericReplicatedTableMeta)

////////////////////////////////////////////////////////////////////////////////

struct TReplicatedTableMappingTableRow
{
    TCrossClusterReference Ref;
    std::optional<NHydra::TRevision> Revision;
    std::optional<NObjectClient::EObjectType> ObjectType;
    TGenericReplicatedTableMetaPtr Meta;

    std::optional<TError> SynchronizationError;

    static std::vector<TReplicatedTableMappingTableRow> ParseRowRange(
        TRange<NTableClient::TUnversionedRow> rows,
        const NTableClient::TNameTablePtr& nameTable);

    static NApi::IUnversionedRowsetPtr InsertRowRange(TRange<TReplicatedTableMappingTableRow> rows);
    static NApi::IUnversionedRowsetPtr DeleteRowRange(TRange<TReplicatedTableMappingTableRow> keys);

    static TReplicatedTableMappingTableRow FromAttributeDictionary(
        const TCrossClusterReference& object,
        const NYTree::IAttributeDictionaryPtr& cypressAttributes);

    std::vector<NYPath::TRichYPath> GetReplicas(
        std::optional<NTabletClient::ETableReplicaMode> mode = {},
        std::optional<NTabletClient::ETableReplicaContentType> contentType = {}) const;

    void Validate() const;
};

void Serialize(const TReplicatedTableMappingTableRow& row, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableMappingTable
    : public TTableBase<TReplicatedTableMappingTableRow>
{
public:
    // NB: The constructor takes the full path, instead of the root path.
    // The registration table can be located on a remote cluster, which should be handled by passing the correct client.
    TReplicatedTableMappingTable(NYPath::TYPath path, NApi::IClientPtr client);
};

DEFINE_REFCOUNTED_TYPE(TReplicatedTableMappingTable)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicState
    : public TRefCounted
{
    TQueueTablePtr Queues;
    TConsumerTablePtr Consumers;
    TQueueAgentObjectMappingTablePtr QueueAgentObjectMapping;
    TConsumerRegistrationTablePtr Registrations;

    //! Might be null for queue agents that are not supposed to fill this table.
    TReplicatedTableMappingTablePtr ReplicatedTableMapping;

    TDynamicState(
        const TQueueAgentDynamicStateConfigPtr& config,
        const NApi::IClientPtr& localClient,
        const NHiveClient::TClientDirectoryPtr& clientDirectory);
};

DEFINE_REFCOUNTED_TYPE(TDynamicState)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
