#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/catalog_entry/dependency_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/catalog/dependency_catalog_set.hpp"

namespace duckdb {

static void AssertMangledName(const string &mangled_name, idx_t expected_null_bytes) {
#ifdef DEBUG
	idx_t nullbyte_count = 0;
	for (auto &ch : mangled_name) {
		nullbyte_count += ch == '\0';
	}
	D_ASSERT(nullbyte_count == expected_null_bytes);
#endif
}

MangledEntryName::MangledEntryName(const CatalogEntryInfo &info) {
	static const auto NULL_BYTE = string(1, '\0');

	auto &type = info.type;
	auto &schema = info.schema;
	auto &name = info.name;

	this->name = CatalogTypeToString(type) + NULL_BYTE + schema + NULL_BYTE + name;
	AssertMangledName(this->name, 2);
}

MangledDependencyName::MangledDependencyName(const MangledEntryName &from, const MangledEntryName &to) {
	static const auto NULL_BYTE = string(1, '\0');
	this->name = from.name + NULL_BYTE + to.name;
	AssertMangledName(this->name, 5);
}

DependencyManager::DependencyManager(DuckCatalog &catalog)
    : catalog(catalog), dependencies(catalog), dependents(catalog) {
}

string DependencyManager::GetSchema(const CatalogEntry &entry) {
	if (entry.type == CatalogType::SCHEMA_ENTRY) {
		return entry.name;
	}
	return entry.ParentSchema().name;
}

MangledEntryName DependencyManager::MangleName(const CatalogEntryInfo &info) {
	return MangledEntryName(info);
}

MangledEntryName DependencyManager::MangleName(const CatalogEntry &entry) {
	if (entry.type == CatalogType::DEPENDENCY_ENTRY) {
		auto &dependency_entry = entry.Cast<DependencyCatalogEntry>();
		return dependency_entry.MangledName();
	}
	auto type = entry.type;
	auto schema = GetSchema(entry);
	auto name = entry.name;
	CatalogEntryInfo info {type, schema, name};

	return MangleName(info);
}

DependencyInfo DependencyInfo::FromDependency(DependencyCatalogEntry &dep) {
	return DependencyInfo {/*dependent = */ dep.FromInfo(),
	                       /*dependency = */ dep.EntryInfo(),
	                       /*dependent_flags = */ DependencyFlags().SetBlocking(),
	                       /*dependency_flags = */ dep.Flags()};
}

DependencyInfo DependencyInfo::FromDependent(DependencyCatalogEntry &dep) {
	return DependencyInfo {/*dependent = */ dep.EntryInfo(),
	                       /*dependency = */ dep.FromInfo(),
	                       /*dependent_flags = */ dep.Flags(),
	                       /*dependency_flags = */ DependencyFlags().SetBlocking()};
}

// ----------- DEPENDENCY_MANAGER -----------

bool DependencyManager::IsSystemEntry(CatalogEntry &entry) const {
	if (entry.type != CatalogType::SCHEMA_ENTRY && entry.internal) {
		// We do create dependency sets for Schemas, they would be created at a later time regardless
		// and that could cause a write-write conflict if used in separate connections
		return true;
	}

	switch (entry.type) {
	case CatalogType::DEPENDENCY_ENTRY:
	case CatalogType::DATABASE_ENTRY:
	case CatalogType::RENAMED_ENTRY:
		return true;
	default:
		return false;
	}
}

CatalogSet &DependencyManager::Dependents() {
	return dependents;
}

CatalogSet &DependencyManager::Dependencies() {
	return dependencies;
}

void DependencyManager::ScanSetInternal(CatalogTransaction transaction, const CatalogEntryInfo &info,
                                        bool scan_dependency, dependency_callback_t &callback) {
	catalog_entry_set_t other_entries;
	DependencyCatalogSet dependents(Dependents(), info);
	DependencyCatalogSet dependencies(Dependencies(), info);

	auto cb = [&](CatalogEntry &other) {
		D_ASSERT(other.type == CatalogType::DEPENDENCY_ENTRY);
		auto &other_entry = other.Cast<DependencyCatalogEntry>();

		other_entries.insert(other_entry);
		callback(other_entry);
	};

	if (scan_dependency) {
		dependencies.Scan(transaction, cb);
	} else {
		dependents.Scan(transaction, cb);
	}

#ifdef DEBUG
	// Verify some invariants
	// Every dependency should have a matching dependent in the other set
	// And vice versa
	auto mangled_name = MangleName(info);

	if (scan_dependency) {
		for (auto &entry : other_entries) {
			auto other_info = GetLookupProperties(entry);
			DependencyCatalogSet other_dependents(Dependents(), other_info);

			// Verify that the other half of the dependency also exists
			auto dependent = other_dependents.GetEntryDetailed(transaction, mangled_name);
			D_ASSERT(dependent.reason != CatalogSet::EntryLookup::FailureReason::NOT_PRESENT);
		}
	} else {
		for (auto &entry : other_entries) {
			auto other_info = GetLookupProperties(entry);
			DependencyCatalogSet other_dependencies(Dependencies(), other_info);

			// Verify that the other half of the dependent also exists
			auto dependency = other_dependencies.GetEntryDetailed(transaction, mangled_name);
			D_ASSERT(dependency.reason != CatalogSet::EntryLookup::FailureReason::NOT_PRESENT);
		}
	}
#endif
}

void DependencyManager::ScanDependents(CatalogTransaction transaction, const CatalogEntryInfo &info,
                                       dependency_callback_t &callback) {
	ScanSetInternal(transaction, info, false, callback);
}

void DependencyManager::ScanDependencies(CatalogTransaction transaction, const CatalogEntryInfo &info,
                                         dependency_callback_t &callback) {
	ScanSetInternal(transaction, info, true, callback);
}

void DependencyManager::RemoveDependency(CatalogTransaction transaction, const DependencyInfo &info) {
	auto &dependent = info.dependent;
	auto &dependency = info.dependency;

	// The dependents of the dependency (target)
	DependencyCatalogSet dependents(Dependents(), dependency);
	// The dependencies of the dependent (initiator)
	DependencyCatalogSet dependencies(Dependencies(), dependent);

	auto dependent_mangled = MangledEntryName(dependent);
	auto dependency_mangled = MangledEntryName(dependency);

	auto dependent_p = dependents.GetEntry(transaction, dependent_mangled);
	if (dependent_p) {
		// 'dependent' is no longer inhibiting the deletion of 'dependency'
		dependents.DropEntry(transaction, dependent_mangled, false);
	}
	auto dependency_p = dependencies.GetEntry(transaction, dependency_mangled);
	if (dependency_p) {
		// 'dependency' is no longer required by 'dependent'
		dependencies.DropEntry(transaction, dependency_mangled, false);
	}
}

void DependencyManager::CreateDependencyInternal(CatalogTransaction transaction, CatalogSet &catalog_set,
                                                 const CatalogEntryInfo &to, const CatalogEntryInfo &from,
                                                 DependencyFlags flags) {
	DependencyCatalogSet dependents(catalog_set, from);

	auto dependent_name = MangleName(to);
	auto existing = dependents.GetEntry(transaction, dependent_name);
	if (existing) {
		auto &existing_flags = existing->Cast<DependencyCatalogEntry>().Flags();
		if (flags == existing_flags) {
			return;
		}
		flags.Apply(existing_flags);
		dependents.DropEntry(transaction, dependent_name, false, false);
	}
	auto dependent_p = make_uniq<DependencyCatalogEntry>(catalog, to, from, flags);

	D_ASSERT(!StringUtil::CIEquals(dependent_name.name, MangleName(from).name));
	if (catalog.IsTemporaryCatalog()) {
		dependent_p->temporary = true;
	}
	dependents.CreateEntry(transaction, dependent_name, std::move(dependent_p));
}

void DependencyManager::CreateDependency(CatalogTransaction transaction, const DependencyInfo &info) {
	auto &dependent = info.dependent;
	auto &dependency = info.dependency;

	// Create an entry in the dependents map of the object that is the target of the dependency
	CreateDependencyInternal(transaction, Dependents(), dependent, dependency, info.dependent_flags);
	// Create an entry in the dependencies map of the object that is targeting another entry
	CreateDependencyInternal(transaction, Dependencies(), dependency, dependent, info.dependency_flags);
}

void DependencyManager::CreateDependencies(CatalogTransaction transaction, const CatalogEntry &object,
                                           const LogicalDependencyList unfiltered_dependencies) {
	DependencyFlags dependency_flags;
	if (object.type != CatalogType::INDEX_ENTRY) {
		// indexes do not require CASCADE to be dropped, they are simply always dropped along with the table
		dependency_flags.SetBlocking();
	}

	const auto object_info = GetLookupProperties(object);
	LogicalDependencyList dependencies;
	// check for each object in the sources if they were not deleted yet
	for (auto &dependency : unfiltered_dependencies.Set()) {
		if (dependency.catalog != object.ParentCatalog().GetName()) {
			continue;
		}
		if (object_info == dependency.entry) {
			continue;
		}
		dependencies.AddDependency(dependency);
	}

	// add the object to the dependents_map of each object that it depends on
	for (auto &dependency : dependencies.Set()) {
		// Create the dependent and complete the link by creating the dependency as well
		DependencyInfo info {/*dependent = */ GetLookupProperties(object),
		                     /*dependency = */ dependency.entry,
		                     /*dependent_flags = */ dependency_flags,
		                     /*dependency_flags = */ DependencyFlags().SetBlocking()};
		CreateDependency(transaction, info);
	}
}

void DependencyManager::AddObject(CatalogTransaction transaction, CatalogEntry &object,
                                  const LogicalDependencyList &dependencies) {
	if (IsSystemEntry(object)) {
		// Don't do anything for this
		return;
	}

	CreateDependencies(transaction, object, dependencies);
}

static bool CascadeDrop(bool cascade, const DependencyFlags &flags) {
	if (cascade) {
		return true;
	}
	if (flags.IsOwnership()) {
		// We own this object, it's automatically dropped
		return true;
	}
	if (flags.IsOwned()) {
		// We are owned by this object, while it exists we can not be dropped without cascade.
		return false;
	}
	return !flags.IsBlocking();
}

CatalogEntryInfo DependencyManager::GetLookupProperties(const CatalogEntry &entry) {
	if (entry.type == CatalogType::DEPENDENCY_ENTRY) {
		auto &dependency_entry = entry.Cast<DependencyCatalogEntry>();

		auto &schema = dependency_entry.EntrySchema();
		auto &name = dependency_entry.EntryName();
		auto type = dependency_entry.EntryType();
		return CatalogEntryInfo {type, schema, name};
	} else {
		auto schema = DependencyManager::GetSchema(entry);
		auto &name = entry.name;
		auto &type = entry.type;
		return CatalogEntryInfo {type, schema, name};
	}
}

optional_ptr<CatalogEntry> DependencyManager::LookupEntry(CatalogTransaction transaction, CatalogEntry &dependency) {
	auto info = GetLookupProperties(dependency);

	auto &type = info.type;
	auto &schema = info.schema;
	auto &name = info.name;

	// Lookup the schema
	auto schema_entry = catalog.GetSchema(transaction, schema, OnEntryNotFound::RETURN_NULL);
	if (type == CatalogType::SCHEMA_ENTRY || !schema_entry) {
		// This is a schema entry, perform the callback only providing the schema
		return reinterpret_cast<CatalogEntry *>(schema_entry.get());
	}
	auto entry = schema_entry->GetEntry(transaction, type, name);
	return entry;
}

void DependencyManager::CleanupDependencies(CatalogTransaction transaction, CatalogEntry &object) {
	// Collect the dependencies
	vector<DependencyInfo> to_remove;

	auto info = GetLookupProperties(object);
	ScanDependencies(transaction, info,
	                 [&](DependencyCatalogEntry &dep) { to_remove.push_back(DependencyInfo::FromDependency(dep)); });
	ScanDependents(transaction, info,
	               [&](DependencyCatalogEntry &dep) { to_remove.push_back(DependencyInfo::FromDependent(dep)); });

	// Remove the dependency entries
	for (auto &dep : to_remove) {
		RemoveDependency(transaction, dep);
	}
}

void DependencyManager::DropObject(CatalogTransaction transaction, CatalogEntry &object, bool cascade) {
	if (IsSystemEntry(object)) {
		// Don't do anything for this
		return;
	}

	auto info = GetLookupProperties(object);
	// Check if there are any entries that block the DROP because they still depend on the object
	catalog_entry_set_t to_drop;
	ScanDependents(transaction, info, [&](DependencyCatalogEntry &dep) {
		// It makes no sense to have a schema depend on anything
		D_ASSERT(dep.EntryType() != CatalogType::SCHEMA_ENTRY);
		auto entry = LookupEntry(transaction, dep);
		if (!entry) {
			return;
		}

		if (!CascadeDrop(cascade, dep.Flags())) {
			// no cascade and there are objects that depend on this object: throw error
			throw DependencyException("Cannot drop entry \"%s\" because there are entries that "
			                          "depend on it. Use DROP...CASCADE to drop all dependents.",
			                          object.name);
		}
		to_drop.insert(*entry);
	});

	CleanupDependencies(transaction, object);

	for (auto &entry : to_drop) {
		auto set = entry.get().set;
		D_ASSERT(set);
		set->DropEntry(transaction, entry.get().name, cascade);
	}
}

void DependencyManager::AlterObject(CatalogTransaction transaction, CatalogEntry &old_obj, CatalogEntry &new_obj,
                                    const LogicalDependencyList &added_dependencies) {
	if (IsSystemEntry(new_obj)) {
		D_ASSERT(IsSystemEntry(old_obj));
		// Don't do anything for this
		return;
	}

	const auto old_info = GetLookupProperties(old_obj);
	const auto new_info = GetLookupProperties(new_obj);

	vector<DependencyInfo> dependencies;
	// Other entries that depend on us
	ScanDependents(transaction, old_info, [&](DependencyCatalogEntry &dep) {
		// It makes no sense to have a schema depend on anything
		D_ASSERT(dep.EntryType() != CatalogType::SCHEMA_ENTRY);

		if (dep.EntryType() == CatalogType::INDEX_ENTRY) {
			// FIXME: this is only done because the table name is baked into the SQL of the Index Entry
			// If we update that then there is no reason this has to throw an exception.

			// conflict: attempting to alter this object but the dependent object still exists
			// no cascade and there are objects that depend on this object: throw error
			throw DependencyException("Cannot alter entry \"%s\" because there are entries that "
			                          "depend on it.",
			                          old_obj.name);
		}

		D_ASSERT(old_info == dep.FromInfo());
		dependencies.emplace_back(DependencyInfo {/*dependent = */ dep.EntryInfo(),
		                                          /*dependency = */ new_info,
		                                          /*dependent_flags = */ dep.Flags(),
		                                          /*dependency_flags = */ DependencyFlags().SetBlocking()});

		// Re-establish the ownership connection
		if (dep.Flags().IsOwnership()) {
			dependencies.emplace_back(DependencyInfo {/*dependent = */ new_info,
			                                          /*dependency = */ dep.EntryInfo(),
			                                          /*dependent_flags = */ DependencyFlags().SetOwned(),
			                                          /*dependency_flags = */ DependencyFlags().SetBlocking()});
		}
	});

	// Entries that we depend on
	ScanDependencies(transaction, old_info, [&](DependencyCatalogEntry &dep) {
		auto entry = LookupEntry(transaction, dep);
		if (!entry) {
			return;
		}
		DependencyCatalogSet other_dependents(Dependents(), dep.EntryInfo());

		// Find the dependent entry so we can properly restore the type it has
		auto old_mangled = MangleName(old_obj);
		auto &dependent = other_dependents.GetEntry(transaction, old_mangled)->Cast<DependencyCatalogEntry>();

		D_ASSERT(old_info == dep.FromInfo());
		const auto dep_info = DependencyInfo {/*dependent = */ new_info,
		                                      /*dependency = */ dep.EntryInfo(),
		                                      /*dependent_flags = */ dependent.Flags(),
		                                      /*dependency_flags = */ DependencyFlags().SetBlocking()};
		dependencies.emplace_back(dep_info);
	});

	//// Add the additional dependencies introduced by the ALTER statement
	// CreateDependencies(transaction, new_obj, added_dependencies);

	// FIXME: we should update dependencies in the future
	// some alters could cause dependencies to change (imagine types of table columns)
	// or DEFAULT depending on a sequence
	if (!StringUtil::CIEquals(old_obj.name, new_obj.name)) {
		// The name has been changed, we need to recreate the dependency links
		CleanupDependencies(transaction, old_obj);
	}

	// Reinstate the old dependencies
	for (auto &dep : dependencies) {
		CreateDependency(transaction, dep);
	}
}

bool AllExportDependenciesWritten(optional_ptr<CatalogTransaction> transaction,
                                  const catalog_entry_vector_t &dependencies, catalog_entry_set_t &exported) {
	for (auto &entry : dependencies) {
		// This is an entry that needs to be written before 'object' can be written
		bool contains = false;
		for (auto &to_check : exported) {
			LogicalDependency a(entry);
			LogicalDependency b(to_check);

			if (a == b) {
				contains = true;
				break;
			}
			// 'a' not found in exported, check outliers
			if (entry.get().type != CatalogType::DEPENDENCY_ENTRY) {
				continue;
			}
			auto &dep = entry.get().Cast<DependencyCatalogEntry>();
			auto &dependent = dep.GetLink(transaction);
			auto &flags = dependent.Flags();
			if (flags.IsOwnership() && !flags.IsBlocking()) {
				// 'object' is owned by this entry
				// it needs to be written first
				contains = true;
				break;
			}
			continue;
		}
		if (!contains) {
			return false;
		}
		// We do not need to check recursively, if the object is written
		// that means that the objects it depends on have also been written
	}
	return true;
}

void AddDependentsToBacklog(stack<reference<CatalogEntry>> &backlog, const catalog_entry_vector_t &dependents) {
	catalog_entry_vector_t tables;
	for (auto &dependent : dependents) {
		backlog.push(dependent);
	}
}

catalog_entry_vector_t DependencyManager::GetExportOrder(optional_ptr<CatalogTransaction> transaction_p) {
	CatalogEntryOrdering ordering;
	auto &entries = ordering.ordered_set;
	auto &export_order = ordering.ordered_vector;

	auto &transaction = *transaction_p;

	stack<reference<CatalogEntry>> backlog;
	dependents.Scan(transaction, [](CatalogEntry &entry) { backlog.push(entry); });

	while (!backlog.empty()) {
		// As long as we still have unordered entries
		auto &object = backlog.top().Cast<DependencyCatalogEntry>();
		backlog.pop();
		auto it = std::find_if(entries.begin(), entries.end(), [&](CatalogEntry &to_check_p) {
			return MangledName(to_check_p) == MangledName(object);
		});
		if (it != entries.end()) {
			// This entry has already been written
			continue;
		}

		auto &info = object.FromInfo();
		catalog_entry_vector_t dependencies;
		DependencyCatalogSet dependencies_map(Dependencies(), info);
		dependencies_map.Scan(transaction, [](CatalogEntry &entry) { dependencies.push_back(entry); }) auto is_ordered =
		    AllExportDependenciesWritten(transaction, dependencies, entries);
		if (!is_ordered) {
			for (auto &dependency : dependencies) {
				backlog.emplace(dependency);
			}
			continue;
		}
		// All dependencies written, we can write this now
		auto insert_result = entries.insert(object);
		(void)insert_result;
		D_ASSERT(insert_result.second);
		auto entry = LookupEntry(transaction, object);
		export_order.push_back(*entry);
		auto dependents = set.GetEntriesThatDependOnUs(transaction);
		AddDependentsToBacklog(backlog, dependents);
	}

	return std::move(ordering.ordered_vector);
}

void DependencyManager::Scan(ClientContext &context,
                             const std::function<void(CatalogEntry &, CatalogEntry &, DependencyFlags)> &callback) {
	// FIXME: why do we take the write_lock here??
	lock_guard<mutex> write_lock(catalog.GetWriteLock());
	auto transaction = catalog.GetCatalogTransaction(context);

	// All the objects registered in the dependency manager
	catalog_entry_set_t entries;
	dependents.Scan(transaction, [&](CatalogEntry &set) {
		auto entry = LookupEntry(transaction, set);
		entries.insert(*entry);
	});

	// For every registered entry, get the dependents
	for (auto &entry : entries) {
		auto entry_info = GetLookupProperties(entry);
		// Scan all the dependents of the entry
		ScanDependents(transaction, entry_info, [&](DependencyCatalogEntry &dependent) {
			auto dep = LookupEntry(transaction, dependent);
			if (!dep) {
				return;
			}
			auto &dependent_entry = *dep;
			callback(entry, dependent_entry, dependent.Flags());
		});
	}
}

void DependencyManager::AddOwnership(CatalogTransaction transaction, CatalogEntry &owner, CatalogEntry &entry) {
	if (IsSystemEntry(entry) || IsSystemEntry(owner)) {
		return;
	}

	// If the owner is already owned by something else, throw an error
	const auto owner_info = GetLookupProperties(owner);
	const auto mangled_owner_name = MangleName(owner_info);
	ScanDependents(transaction, owner_info, [&](DependencyCatalogEntry &dep) {
		if (dep.Flags().IsOwned()) {
			throw DependencyException(owner.name + " already owned by " + dep.EntryName());
		}
	});

	// If the entry is already owned, throw an error
	const auto entry_info = GetLookupProperties(entry);
	ScanDependents(transaction, entry_info, [&](DependencyCatalogEntry &other) {
		auto flags = other.Flags();

		if (other.MangledName() != mangled_owner_name) {
			// FIXME: this is much too broad, we should create a function to check for recursive dependencies instead.
			throw DependencyException(entry.name + " already depends on " + other.EntryName());
		}

		if (flags.IsOwned()) {
			if (other.MangledName() == mangled_owner_name) {
				return;
			}
			throw DependencyException(entry.name + " already depends on " + other.EntryName());
		} else if (flags.IsOwnership()) {
			// This entry is the owner of another entry
			if (other.MangledName() != mangled_owner_name) {
				return;
			}
			throw DependencyException("%s already owns %s. Can not have circular dependencies.", entry.name,
			                          owner.name);
		}
	});
	{
		DependencyInfo info {/*dependent = */ GetLookupProperties(owner),
		                     /*dependency = */ GetLookupProperties(entry),
		                     /*dependent_flags = */ DependencyFlags().SetOwned(),
		                     /*dependency_flags = */ DependencyFlags().SetBlocking()};
		CreateDependency(transaction, info);
	}
	{
		DependencyInfo info {/*dependent = */ GetLookupProperties(entry),
		                     /*dependency = */ GetLookupProperties(owner),
		                     /*dependent_flags = */ DependencyFlags().SetOwnership(),
		                     /*dependency_flags = */ DependencyFlags().SetBlocking()};
		CreateDependency(transaction, info);
	}
}

} // namespace duckdb
