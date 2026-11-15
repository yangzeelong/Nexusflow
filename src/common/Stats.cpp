#include <nexusflow/Stats.hpp>

#include <sstream>

namespace nexusflow {

std::string PipelineSnapshot::ToString() const {
    std::ostringstream out;
    out << "pipeline '" << name << "' edges: [";
    for (std::size_t i = 0; i < edges.size(); ++i) {
        const EdgeSnapshot& edge = edges[i];
        if (i > 0) {
            out << ' ';
        }
        out << edge.from << "->" << edge.to << " size=" << edge.size << '/'
            << (edge.capacity == 0 ? std::string("inf") : std::to_string(edge.capacity)) << " max=" << edge.maxSize
            << " pushed=" << edge.pushedTotal << " popped=" << edge.poppedTotal;
    }
    out << "] modules: [";
    for (std::size_t i = 0; i < modules.size(); ++i) {
        const ModuleSnapshot& module = modules[i];
        if (i > 0) {
            out << ' ' << '|' << ' ';
        }
        out << module.name << " processed=" << module.processedTotal << " dropped=" << module.droppedTotal
            << " fusionExpired=" << module.fusionExpiredTotal << " failed=" << module.failedTotal;
    }
    out << ']';
    return out.str();
}

} // namespace nexusflow
