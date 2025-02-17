#include "private.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

const TString CacheUserName("yt-clickhouse-cache");
const TString ChytSqlObjectsUserName("chyt-sql-objects");
const TString InternalRemoteUserName("$remote");
const std::vector<std::string> TableAttributesToFetch{
    "id",
    // TODO(dakovalkov): Eliminate this with "schema_id" (CHYT-687).
    "schema",
    "type",
    "dynamic",
    "chunk_count",
    "external",
    "external_cell_tag",
    "revision",
    "boundary_keys",
    "enable_dynamic_store_read",
    "chyt_banned",
    "row_count",
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

// This is an override of weak symbol from Common/Allocator.h.
// We do not want ClickHouse allocator to use raw mmaps as ytalloc already
// does that by himself.
__attribute__((__used__)) extern const size_t MMAP_THRESHOLD = static_cast<size_t>(1) << 60;
