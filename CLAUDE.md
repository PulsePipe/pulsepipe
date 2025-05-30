# CLAUDE.md - Enhanced Development Instructions

## Project Overview
PulsePipe is a modular, AI-native healthcare data pipeline that processes clinical data formats (HL7 v2, FHIR, CDA/CCDA, X12, plain text), de-identifies and normalizes it, and prepares it for LLM processing and vector embeddings.

**⚠️ CRITICAL PERFORMANCE CONTEXT**: PulsePipe currently has significant CLI performance issues with 7+ second startup times for most commands. Performance optimization is a top priority.

## Performance Optimization Roadmap: CLI Command Performance

### Current Status
✅ **Help commands fixed** - `pulsepipe metrics --help` now works and is fast  
❌ **Core commands still slow** - `pulsepipe metrics` and `pulsepipe config` are still taking 7+ seconds

### Critical Performance Issues
PulsePipe is fundamentally a slow CLI that needs deep architectural optimization. The help commands are now fast, but the actual functionality commands remain painfully slow.

### Performance Optimization Targets
1. **`pulsepipe metrics`** - Currently 7+ seconds → Target: < 2 seconds
2. **`pulsepipe config`** - Currently 7+ seconds → Target: < 2 seconds  
3. **General CLI responsiveness** - Make all commands sub-2 seconds

### Key Optimization Strategies
- Implement aggressive lazy loading
- Defer database connections
- Optimize configuration loading
- Minimize startup dependencies
- Profile and eliminate bottlenecks

### Performance Investigation Areas
- Heavy imports blocking startup
- Unnecessary database connections
- Configuration parsing overhead
- Pre-loading of ML models and embeddings
- Expensive startup routines

## Detailed Performance Optimization Analysis

### 1. Startup Bottleneck Investigation
**Profile the entire application startup sequence:**
```bash
# Use Python profiling to identify bottlenecks
python -m cProfile -o profile.stats -c "import pulsepipe; pulsepipe.cli.main()"
```
- **Heavy imports**: Identify which modules are causing the 7-second delays
- **Database connections**: Check if commands are unnecessarily connecting to databases
- **Configuration loading**: See if large config files are being parsed multiple times
- **Model loading**: Check if ML models or embeddings are being loaded upfront

### 2. Architecture Analysis
**Examine the core application architecture:**
- **Dependency injection**: Look for heavy dependencies being loaded at startup
- **Plugin system**: Check if all plugins are being loaded regardless of command
- **Database initialization**: Verify database connections aren't blocking startup
- **File system scanning**: Look for expensive directory traversals or file operations
- **Network calls**: Check for API calls or remote connections during startup

### 3. Import Optimization Strategy
**Apply aggressive lazy loading:**
```python
# Instead of top-level imports
import pandas as pd
import numpy as np
from some_heavy_ml_library import Model

# Use function-level imports
def metrics_command():
    import pandas as pd  # Only when needed
    import numpy as np
    # ... rest of function
```

### 4. Configuration System Overhaul
**Optimize configuration loading:**
- **Lazy config loading**: Only load config sections when needed
- **Config caching**: Cache parsed configuration in memory
- **Validation deferral**: Skip expensive validation for simple commands
- **Profile-based loading**: Only load relevant config profiles

### 5. Database Connection Management
**Optimize database interactions:**
- **Connection pooling**: Implement proper connection pooling
- **Lazy connections**: Don't connect until absolutely necessary
- **Command-specific connections**: Only connect to databases the command actually needs
- **Connection caching**: Reuse connections across command executions

### 6. Model and Embedding Optimization
**If ML models are causing slowness:**
- **Model lazy loading**: Load models only when doing actual inference
- **Model caching**: Cache loaded models in memory for reuse
- **Lightweight alternatives**: Use smaller models for non-critical operations
- **Background loading**: Load heavy models in background threads

## Implementation Priority

### Phase 1: Profiling and Root Cause Analysis (High Priority)
1. **Profile actual command execution** - Not just imports, but full command lifecycle
2. **Identify the top 3 bottlenecks** - Focus on the biggest time consumers
3. **Database connection analysis** - Check if DB calls are blocking startup
4. **File I/O analysis** - Look for expensive file operations

### Phase 2: Targeted Optimizations (High Priority)  
1. **Eliminate startup blockers** - Remove anything that doesn't need to happen at startup
2. **Implement command-specific loading** - Each command should only load what it needs
3. **Cache expensive operations** - Cache anything that can be reused
4. **Async initialization** - Move non-critical setup to background

### Phase 3: Architecture Improvements (Medium Priority)
1. **Plugin system optimization** - Load plugins on-demand
2. **Configuration system redesign** - Make config loading truly lazy
3. **Connection management** - Implement proper connection lifecycle
4. **Background services** - Move heavy operations to background processes

## Specific Investigation Areas

### Database/Storage Layer
- Are database connections being established unnecessarily?
- Is the application scanning large directories or files?
- Are there expensive database queries running during startup?

### ML/AI Components  
- Are embedding models being loaded during startup?
- Is the application connecting to vector databases unnecessarily?
- Are there expensive model initialization routines?

### Configuration System
- Is the entire configuration being validated on every command?
- Are large configuration files being parsed multiple times?
- Is configuration loading triggering expensive checks?

## Performance Measurement
**Before optimization, measure:**
```bash
time pulsepipe metrics          # Current baseline
time pulsepipe config           # Current baseline  
time pulsepipe config validate  # Current baseline
```

**After optimization, verify:**
- All commands complete in < 2 seconds
- Functionality remains intact
- Unit test coverage stays ≥ 85%

## Expected Outcomes
🎯 **Target Performance:**
- `pulsepipe metrics`: < 2 seconds (from 7+ seconds)
- `pulsepipe config`: < 2 seconds (from 7+ seconds)
- All other commands: < 2 seconds
- Cold start performance: < 3 seconds for any command

🔍 **Root Cause Identification:**
- Identify the specific components causing 7-second delays
- Document performance bottlenecks for future optimization
- Create performance regression tests

⚡ **Architectural Improvements:**
- Implement true lazy loading throughout the application
- Optimize the critical path for common operations
- Establish performance monitoring for future changes

## Testing Requirements
- **Maintain 85% unit test coverage** (tests in `tests/`)
- **Performance regression tests** for all optimized commands
- **Functionality verification** - ensure no features are broken
- **Cross-platform testing** - verify improvements work on all platforms