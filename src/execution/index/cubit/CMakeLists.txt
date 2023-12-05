add_library_unity(
  duckdb_execution_index_cubit
  OBJECT
  cubit.cpp
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES}
    $<TARGET_OBJECTS:duckdb_execution_index_cubit>
    PARENT_SCOPE)