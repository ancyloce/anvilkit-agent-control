package grpc

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

type preparationServer struct {
	controlv1.UnimplementedPreparationServiceServer
	preparations *application.Preparations
}

var questionSetStateToProto = map[domain.QuestionSetState]controlv1.QuestionSetState{
	domain.QuestionSetOpen: controlv1.QuestionSetState_QUESTION_SET_STATE_OPEN, domain.QuestionSetAnswered: controlv1.QuestionSetState_QUESTION_SET_STATE_ANSWERED,
	domain.QuestionSetExpired: controlv1.QuestionSetState_QUESTION_SET_STATE_EXPIRED, domain.QuestionSetSuperseded: controlv1.QuestionSetState_QUESTION_SET_STATE_SUPERSEDED,
}

var answerRelayToProto = map[domain.AnswerRelayState]controlv1.RelayState{
	domain.AnswerRelayPending: controlv1.RelayState_RELAY_STATE_PENDING, domain.AnswerRelaySent: controlv1.RelayState_RELAY_STATE_SENT,
	domain.AnswerRelayApplied: controlv1.RelayState_RELAY_STATE_APPLIED, domain.AnswerRelayRejected: controlv1.RelayState_RELAY_STATE_REJECTED,
}

var answerRelayFromProto = map[controlv1.RelayState]domain.AnswerRelayState{
	controlv1.RelayState_RELAY_STATE_PENDING: domain.AnswerRelayPending, controlv1.RelayState_RELAY_STATE_SENT: domain.AnswerRelaySent,
	controlv1.RelayState_RELAY_STATE_APPLIED: domain.AnswerRelayApplied, controlv1.RelayState_RELAY_STATE_REJECTED: domain.AnswerRelayRejected,
}

var briefStateToProto = map[domain.BriefState]controlv1.BriefState{
	domain.BriefCurrent: controlv1.BriefState_BRIEF_STATE_CURRENT, domain.BriefSuperseded: controlv1.BriefState_BRIEF_STATE_SUPERSEDED,
}

func toQuestionSet(qs *domain.QuestionSet) *controlv1.QuestionSet {
	out := &controlv1.QuestionSet{
		QuestionSetId: qs.ID, OperationId: qs.OperationID, Revision: domain.Revision(qs.Revision).String(), Round: domain.Revision(qs.Round).String(),
		AskedAt: timestamppb.New(qs.AskedAt), ExpiresAt: timestamppb.New(qs.ExpiresAt), State: questionSetStateToProto[qs.State],
	}
	for _, q := range qs.Questions {
		out.Questions = append(out.Questions, &controlv1.Question{QuestionId: q.ID, Text: q.Text})
	}
	return out
}

func toAnswer(a *domain.Answer) *controlv1.Answer {
	out := &controlv1.Answer{
		AnswerId: a.ID, OperationId: a.OperationID, QuestionSetId: a.QuestionSetID, QuestionSetRevision: domain.Revision(a.QuestionSetRevision).String(),
		Answer: &controlv1.ArtifactBinding{TransferId: a.Answer.TransferID, Digest: string(a.Answer.Digest)}, Handle: a.Handle, UpdateId: a.UpdateID,
		Relay: answerRelayToProto[a.Relay], AcceptedAt: timestamppb.New(a.AcceptedAt),
	}
	if a.RelayedAt != nil {
		out.RelayedAt = timestamppb.New(*a.RelayedAt)
	}
	return out
}

func toSourceRefs(refs []domain.SourceReference) []*controlv1.SourceReference {
	out := make([]*controlv1.SourceReference, 0, len(refs))
	for _, r := range refs {
		out = append(out, &controlv1.SourceReference{SourceId: r.SourceID, Revision: domain.Revision(r.Revision).String()})
	}
	return out
}

func toContentDigests(ds []domain.ContentDigest) []*controlv1.ContentDigest {
	out := make([]*controlv1.ContentDigest, 0, len(ds))
	for _, d := range ds {
		out = append(out, &controlv1.ContentDigest{SourceId: d.SourceID, Revision: domain.Revision(d.Revision).String(), Digest: string(d.Digest)})
	}
	return out
}

func toBrief(b *domain.Brief) *controlv1.Brief {
	return &controlv1.Brief{
		BriefId: b.ID, OperationId: b.OperationID, TenantId: b.TenantID, Revision: domain.Revision(b.Revision).String(),
		Brief: &controlv1.ArtifactBinding{TransferId: b.Brief.TransferID, Digest: string(b.Brief.Digest)}, Handle: b.Handle,
		RequirementsDigest: string(b.RequirementsDigest), SourceRevisions: toSourceRefs(b.SourceRevisions), BrandDigests: toContentDigests(b.BrandDigests),
		AssetDigests: toContentDigests(b.AssetDigests), State: briefStateToProto[b.State], FrozenAt: timestamppb.New(b.FrozenAt),
	}
}

func (s *preparationServer) GetPreparation(ctx context.Context, req *controlv1.GetPreparationRequest) (*controlv1.GetPreparationResponse, error) {
	v, err := s.preparations.Get(ctx, req.GetTenantId(), req.GetOperationId())
	if err != nil {
		return nil, toStatus(err)
	}
	op := v.Operation
	out := &controlv1.Preparation{
		OperationId: op.ID, TenantId: op.TenantID, PromptHandle: v.PromptHandle,
		Prompt:          &controlv1.ArtifactBinding{TransferId: op.Preparation.Prompt.TransferID, Digest: string(op.Preparation.Prompt.Digest)},
		BrandReferences: toSourceRefs(op.Preparation.BrandReferences), AssetReferences: toSourceRefs(op.Preparation.AssetReferences),
		MaxRounds: domain.Revision(v.Bounds.MaxRounds).String(), MaxQuestions: domain.Revision(v.Bounds.MaxQuestions).String(),
		WaitSeconds: domain.Revision(uint64(v.Bounds.Wait.Seconds())).String(),
	}
	for _, qs := range v.QuestionSets {
		out.QuestionSets = append(out.QuestionSets, toQuestionSet(qs))
	}
	if v.Brief != nil {
		out.Brief = toBrief(v.Brief)
	}
	return &controlv1.GetPreparationResponse{Preparation: out}, nil
}

func (s *preparationServer) RecordQuestionSet(ctx context.Context, req *controlv1.RecordQuestionSetRequest) (*controlv1.RecordQuestionSetResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	round, err := domain.ParseRevision(req.GetRound())
	if err != nil {
		return nil, toStatus(err)
	}
	questions := make([]domain.Question, 0, len(req.GetQuestions()))
	for _, q := range req.GetQuestions() {
		questions = append(questions, domain.Question{ID: q.GetQuestionId(), Text: q.GetText()})
	}
	qs, existing, err := s.preparations.RecordQuestionSet(ctx, cmd, req.GetOperationId(), uint64(round), questions)
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.RecordQuestionSetResponse{QuestionSet: toQuestionSet(qs), Existing: existing}, nil
}

func (s *preparationServer) SubmitAnswer(ctx context.Context, req *controlv1.SubmitAnswerRequest) (*controlv1.SubmitAnswerResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	revision, err := domain.ParseRevision(req.GetQuestionSetRevision())
	if err != nil {
		return nil, toStatus(err)
	}
	digest, err := domain.ParseDigest(req.GetAnswer().GetDigest())
	if err != nil {
		return nil, toStatus(err)
	}
	a, existing, err := s.preparations.SubmitAnswer(ctx, cmd, scope(req.GetScope()), req.GetOperationId(), req.GetQuestionSetId(), uint64(revision), domain.ArtifactBinding{TransferID: req.GetAnswer().GetTransferId(), Digest: digest})
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.SubmitAnswerResponse{Answer: toAnswer(a), Existing: existing}, nil
}

func (s *preparationServer) GetAnswer(ctx context.Context, req *controlv1.GetAnswerRequest) (*controlv1.GetAnswerResponse, error) {
	a, err := s.preparations.GetAnswer(ctx, req.GetTenantId(), req.GetOperationId(), req.GetAnswerId(), req.GetQuestionSetId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.GetAnswerResponse{Answer: toAnswer(a)}, nil
}

func (s *preparationServer) RecordAnswerRelay(ctx context.Context, req *controlv1.RecordAnswerRelayRequest) (*controlv1.RecordAnswerRelayResponse, error) {
	a, err := s.preparations.RecordAnswerRelay(ctx, req.GetTenantId(), req.GetAnswerId(), answerRelayFromProto[req.GetRelay()])
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.RecordAnswerRelayResponse{Answer: toAnswer(a)}, nil
}

func (s *preparationServer) RecordBrief(ctx context.Context, req *controlv1.RecordBriefRequest) (*controlv1.RecordBriefResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	digest, err := domain.ParseDigest(req.GetBrief().GetDigest())
	if err != nil {
		return nil, toStatus(err)
	}
	requirements, err := domain.ParseDigest(req.GetRequirementsDigest())
	if err != nil {
		return nil, toStatus(err)
	}
	sources := make([]domain.SourceReference, 0, len(req.GetSourceRevisions()))
	for _, r := range req.GetSourceRevisions() {
		rev, err := domain.ParseRevision(r.GetRevision())
		if err != nil {
			return nil, toStatus(err)
		}
		sources = append(sources, domain.SourceReference{SourceID: r.GetSourceId(), Revision: uint64(rev)})
	}
	contentDigests := func(in []*controlv1.ContentDigest) ([]domain.ContentDigest, error) {
		out := make([]domain.ContentDigest, 0, len(in))
		for _, d := range in {
			rev, err := domain.ParseRevision(d.GetRevision())
			if err != nil {
				return nil, err
			}
			dg, err := domain.ParseDigest(d.GetDigest())
			if err != nil {
				return nil, err
			}
			out = append(out, domain.ContentDigest{SourceID: d.GetSourceId(), Revision: uint64(rev), Digest: dg})
		}
		return out, nil
	}
	brands, err := contentDigests(req.GetBrandDigests())
	if err != nil {
		return nil, toStatus(err)
	}
	assets, err := contentDigests(req.GetAssetDigests())
	if err != nil {
		return nil, toStatus(err)
	}
	b, existing, err := s.preparations.RecordBrief(ctx, cmd, req.GetOperationId(), domain.ArtifactBinding{TransferID: req.GetBrief().GetTransferId(), Digest: digest}, requirements, sources, brands, assets)
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.RecordBriefResponse{Brief: toBrief(b), Existing: existing}, nil
}

func (s *preparationServer) GetBrief(ctx context.Context, req *controlv1.GetBriefRequest) (*controlv1.GetBriefResponse, error) {
	b, err := s.preparations.GetBrief(ctx, req.GetTenantId(), req.GetBriefId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.GetBriefResponse{Brief: toBrief(b)}, nil
}
