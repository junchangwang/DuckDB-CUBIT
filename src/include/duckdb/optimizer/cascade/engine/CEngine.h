//---------------------------------------------------------------------------
//	@filename:
//		CEngine.h
//
//	@doc:
//		Optimization engine
//---------------------------------------------------------------------------
#ifndef GPOPT_CEngine_H
#define GPOPT_CEngine_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/search/CMemo.h"
#include "duckdb/optimizer/cascade/search/CSearchStage.h"
#include "duckdb/optimizer/cascade/xforms/CXform.h"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/base/CEnfdOrder.h"

using namespace gpos;
using namespace duckdb;

namespace gpopt
{
// forward declarations
class CGroup;
class CJob;
class CJobFactory;
class CQueryContext;
class COptimizationContext;
class CReqdPropPlan;
class CReqdPropRelational;
class CEnumeratorConfig;

//---------------------------------------------------------------------------
//	@class:
//		CEngine
//
//	@doc:
//		Optimization engine; owns entire optimization workflow
//
//---------------------------------------------------------------------------
class CEngine
{
public:
	// query context
	CQueryContext* m_pqc;

	// search strategy
	duckdb::vector<CSearchStage*> m_search_stage_array;

	// index of current search stage
	ULONG m_ulCurrSearchStage;

	// memo table
	CMemo* m_pmemo;

	// pattern used for adding enforcers
	duckdb::unique_ptr<Operator> m_pexprEnforcerPattern;

	// the following variables are used for maintaining optimization statistics

	// set of activated xforms
	CXformSet* m_xforms;

	// number of calls to each xform
	duckdb::vector<ULONG_PTR*> m_pdrgpulpXformCalls;

	// time consumed by each xform
	duckdb::vector<ULONG_PTR*> m_pdrgpulpXformTimes;

	// number of bindings for each xform
	duckdb::vector<ULONG_PTR*> m_pdrgpulpXformBindings;

	// number of alternatives generated by each xform
	duckdb::vector<ULONG_PTR*> m_pdrgpulpXformResults;

public:
	// initialize query logical expression
	void InitLogicalExpression(duckdb::unique_ptr<Operator> pexpr);

	// insert children of the given expression to memo, and
	// copy the resulting groups to the given group array
	void InsertExpressionChildren(Operator* pexpr, duckdb::vector<CGroup*> &pdrgpgroupChildren, CXform::EXformId exfidOrigin, CGroupExpression* pgexprOrigin);

	// create and schedule the main optimization job
	void ScheduleMainJob(CSchedulerContext* psc, COptimizationContext* poc);

	// check if search has terminated
	bool FSearchTerminated() const
	{
		// at least one stage has completed and achieved required cost
		return (NULL != PssPrevious() && PssPrevious()->FAchievedReqdCost());
	}

	// generate random plan id
	ULLONG UllRandomPlanId(ULONG* seed);

	// extract a plan sample and handle exceptions according to enumerator configurations
	bool FValidPlanSample(CEnumeratorConfig* pec, ULLONG plan_id, Operator** ppexpr);

	// check if all children were successfully optimized
	bool FChildrenOptimized(duckdb::vector<COptimizationContext*> pdrgpoc);

	// check if ayn of the given property enforcing types prohibits enforcement
	static bool FProhibited(duckdb::CEnfdOrder::EPropEnforcingType epetOrder);

	// check whether the given memo groups can be marked as duplicates. This is
	// true only if they have the same logical properties
	static bool FPossibleDuplicateGroups(CGroup* pgroupFst, CGroup* pgroupSnd);

	// check if optimization is possible under the given property enforcing types
	static bool FOptimize(duckdb::CEnfdOrder::EPropEnforcingType epetOrder);

	// unrank the plan with the given 'plan_id' from the memo
	Operator* PexprUnrank(ULLONG plan_id);

public:
	// ctor
	explicit CEngine();
	
	// no copy ctor
	CEngine(const CEngine &) = delete;

	// dtor
	~CEngine();

	// initialize engine with a query context and search strategy
	void Init(CQueryContext* pqc, duckdb::vector<CSearchStage*> search_stage_array);

	// accessor of memo's root group
	CGroup* PgroupRoot() const
	{
		return m_pmemo->PgroupRoot();
	}

	// check if a group is the root one
	bool FRoot(CGroup* pgroup) const
	{
		return (PgroupRoot() == pgroup);
	}

	// insert expression tree to memo
	CGroup* PgroupInsert(CGroup* pgroupTarget, duckdb::unique_ptr<Operator> pexpr, CXform::EXformId exfidOrigin, CGroupExpression* pgexprOrigin, bool fIntermediate);

	// insert a set of xform results into the memo
	void InsertXformResult(CGroup* pgroupOrigin, CXformResult* pxfres, CXform::EXformId exfidOrigin, CGroupExpression* pgexprOrigin, ULONG ulXformTime, ULONG ulNumberOfBindings);

	// add enforcers to the memo
	void AddEnforcers(CGroupExpression* pgexpr, duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexprEnforcers);

	// extract a physical plan from the memo
	Operator* PexprExtractPlan();

	// check required properties;
	// return false if it's impossible for the operator to satisfy one or more
	bool FCheckReqdProps(CExpressionHandle &exprhdl, CReqdPropPlan* prpp, ULONG ulOptReq);

	// check enforceable properties;
	// return false if it's impossible for the operator to satisfy one or more
	bool FCheckEnfdProps(CGroupExpression* pgexpr, COptimizationContext* poc, ULONG ulOptReq, duckdb::vector<COptimizationContext*> pdrgpoc);

	// check if the given expression has valid cte and partition properties
	// with respect to the given requirements
	bool FValidCTEAndPartitionProperties(CExpressionHandle &exprhdl, CReqdPropPlan *prpp);

	// derive statistics
	void DeriveStats();

	// execute operations after exploration completes
	void FinalizeExploration();

	// execute operations after implementation completes
	void FinalizeImplementation();

	// execute operations after search stage completes
	void FinalizeSearchStage();

	// main driver of optimization engine
	void Optimize();

	// merge duplicate groups
	void GroupMerge()
	{
		m_pmemo->GroupMerge();
	}

	// return current search stage
	CSearchStage* PssCurrent() const
	{
		return m_search_stage_array[m_ulCurrSearchStage];
	}

	// current search stage index accessor
	ULONG UlCurrSearchStage() const
	{
		return m_ulCurrSearchStage;
	}

	// return previous search stage
	CSearchStage* PssPrevious() const
	{
		if (0 == m_ulCurrSearchStage)
		{
			return NULL;
		}
		return m_search_stage_array[m_ulCurrSearchStage - 1];
	}

	// number of search stages accessor
	ULONG UlSearchStages() const
	{
		return m_search_stage_array.size();
	}

	// set of xforms of current stage
	CXformSet* PxfsCurrentStage() const
	{
		return m_search_stage_array[m_ulCurrSearchStage]->m_xforms;
	}

	// return array of child optimization contexts corresponding to handle requirements
	duckdb::vector<COptimizationContext*> PdrgpocChildren(CExpressionHandle &exprhdl);

	// build tree map on memo
	MemoTreeMap* Pmemotmap();

	// reset tree map
	void ResetTreeMap()
	{
		m_pmemo->ResetTreeMap();
	}

	// check if parent group expression can optimize child group expression
	bool FOptimizeChild(CGroupExpression* pgexprParent, CGroupExpression* pgexprChild, COptimizationContext* pocChild, EOptimizationLevel eol);

	// determine if a plan, rooted by given group expression, can be safely pruned based on cost bounds
	bool FSafeToPrune(CGroupExpression* pgexpr, CReqdPropPlan* prpp, CCostContext* pccChild, ULONG child_index, double* pcostLowerBound);

	// damp optimization level to process group expressions
	// in the next lower optimization level
	static EOptimizationLevel EolDamp(EOptimizationLevel eol);

	// derive statistics
	static void DeriveStats(CGroup* pgroup, CReqdPropRelational* prprel);

	// return the first group expression in a given group
	static CGroupExpression* PgexprFirst(CGroup* pgroup);

	duckdb::vector<ULONG_PTR*> GetNumberOfBindings();
};	// class CEngine
}  // namespace gpopt
#endif