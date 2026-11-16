package grpc

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// DEC-062, DEC-063: a refinement's and its preparation's base is stated as
// the subject's refinement_base, never as a release's source operation.
func TestViewStatesTheRefinementBase(t *testing.T) {
	op := &domain.Operation{ID: "op_1", Kind: domain.KindRefinement, Subject: domain.Subject{
		ProfileID: "refinement-v1", SubjectDigest: "sha256:lineage", BriefID: "brf_1", SourceHandle: "hdl_base",
		Base: domain.RefinementBase{OperationID: "op_base", Revision: "4"},
	}}
	subject := toView(op).GetSubject()
	require.Equal(t, "op_base", subject.GetRefinementBase().GetSourceOperationId())
	require.Equal(t, "4", subject.GetRefinementBase().GetSourceRevision())
	require.Nil(t, subject.SourceOperationId)
	require.Nil(t, subject.SourceRevision)
	require.Equal(t, "hdl_base", subject.GetSourceHandle())

	op.Subject.Base = domain.RefinementBase{}
	require.Nil(t, toView(op).GetSubject().RefinementBase, "a subject without a base states none")
}
