#pragma once

#include <cstddef>
#include <variant>
#include <vector>

// Node IDs are indices into RepairDAG::nodes, not stored shard indices.
using RepairNodeId = std::size_t;

struct RepairInput
{
    // Zero-based shard index within the extent: data first, then parity.
    // Only available stored shards may appear as inputs. Missing shards may
    // still be reconstructed by computation nodes.
    std::size_t shard_id;
};

struct RepairComputation
{
    // Interpreted by the LRC that generated the plan. For example, an LRC can
    // define an XOR operation or a finite-field linear combination operation.
    std::size_t operation_id;
    std::vector<RepairNodeId> inputs;

    // Code-specific operation metadata, e.g. coefficients in input order.
    // The generating LRC defines the representation, field, and symbol format.
    // This contains no shard data or pointers into execution workspace.
    std::vector<std::byte> parameters;
};

struct RepairNode
{
    // Output length in bytes, for either a retrieved input or a computed value.
    std::size_t byte_size;
    std::variant<RepairInput, RepairComputation> operation;
};

/**
 * A code-specific repair plan for one target shard in one extent.
 *
 * Nodes are in topological order: every computation input references a strictly
 * earlier node. Multiple computations may reference the same node. Each node
 * produces one logical byte buffer; node IDs and stored shard IDs are separate.
 * Input shard IDs are unique, so one arrival supplies one input node.
 *
 * A valid plan is nonempty, result names a node containing the reconstructed
 * target, and every node contributes to that result. Operation IDs and metadata
 * must be understood by the executing LRC. A planner should report inability to
 * repair separately (e.g. std::optional<RepairDAG>), not as an empty valid plan.
 *
 * Construct/optimize the metadata before timing, then keep it immutable during
 * repair. Optimizers that reorder nodes must update all node references.
 *
 * This object owns no data buffers, threads, or mutable execution state. The LRC
 * assigns intermediate storage from its preallocated workspace, assumed large
 * enough for all intermediate calculation buffers. Allocation of that workspace
 * happens outside timing; execution bookkeeping allocations may be timed.
 * Workspace is exclusive to an active repair unless the LRC explicitly supports
 * independent workspace regions for concurrent repairs.
 *
 * Edges describe value dependencies, not a fixed thread schedule or mandatory
 * whole-buffer barriers. The executor may accumulate partial contributions or
 * pipeline byte ranges when the operation permits it, with proper synchronization.
 * Retrieved inputs remain read-only; the final result is written to the caller's
 * output span. All worker accesses must finish before repair returns.
 */
struct RepairDAG
{
    std::size_t target_shard;
    std::vector<RepairNode> nodes;
    RepairNodeId result;
};
