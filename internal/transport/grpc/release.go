package grpc

import (
	"context"
	"fmt"
	"strconv"

	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// releaseServer adapts anvilkit.control.v1 ReleaseService (P21).
type releaseServer struct {
	controlv1.UnimplementedReleaseServiceServer
	releases *application.Releases
}

var releaseStateToProto = map[domain.ReleaseState]controlv1.ReleaseState{
	domain.ReleaseCertifying: controlv1.ReleaseState_RELEASE_STATE_CERTIFYING, domain.ReleaseAwaitingApproval: controlv1.ReleaseState_RELEASE_STATE_AWAITING_APPROVAL,
	domain.ReleasePublishing: controlv1.ReleaseState_RELEASE_STATE_PUBLISHING, domain.ReleasePublished: controlv1.ReleaseState_RELEASE_STATE_PUBLISHED,
	domain.ReleaseActivated: controlv1.ReleaseState_RELEASE_STATE_ACTIVATED, domain.ReleasePartiallyPublished: controlv1.ReleaseState_RELEASE_STATE_PARTIALLY_PUBLISHED,
	domain.ReleaseReconciling: controlv1.ReleaseState_RELEASE_STATE_RECONCILING, domain.ReleaseRejected: controlv1.ReleaseState_RELEASE_STATE_REJECTED,
	domain.ReleaseFailed: controlv1.ReleaseState_RELEASE_STATE_FAILED,
}

var approvalStateToProto = map[domain.ApprovalState]controlv1.ApprovalState{
	domain.ApprovalPending: controlv1.ApprovalState_APPROVAL_STATE_PENDING, domain.ApprovalApproved: controlv1.ApprovalState_APPROVAL_STATE_APPROVED,
	domain.ApprovalRejected: controlv1.ApprovalState_APPROVAL_STATE_REJECTED, domain.ApprovalInvalidated: controlv1.ApprovalState_APPROVAL_STATE_INVALIDATED,
}

var targetStateToProto = map[domain.TargetState]controlv1.TargetState{
	domain.TargetPending: controlv1.TargetState_TARGET_STATE_PENDING, domain.TargetSucceeded: controlv1.TargetState_TARGET_STATE_SUCCEEDED,
	domain.TargetFailed: controlv1.TargetState_TARGET_STATE_FAILED, domain.TargetUnknown: controlv1.TargetState_TARGET_STATE_UNKNOWN,
}

func reverse[K comparable, V comparable](m map[K]V, v V) K {
	var zero K
	for k, x := range m {
		if x == v {
			return k
		}
	}
	return zero
}

func digestRef(a *controlv1.ArtifactDigest) domain.ArtifactDigestRef {
	return domain.ArtifactDigestRef{Digest: domain.Digest(a.GetDigest()), SizeBytes: a.GetSizeBytes()}
}

func toDigestRef(a domain.ArtifactDigestRef) *controlv1.ArtifactDigest {
	return &controlv1.ArtifactDigest{Digest: string(a.Digest), SizeBytes: a.SizeBytes}
}

func subjectOf(s *controlv1.ReleaseSubject) *domain.ReleaseSubject {
	if s == nil {
		return nil
	}
	out := &domain.ReleaseSubject{
		SchemaVersion: 1, ComponentID: s.GetComponentId(), PuckType: s.GetPuckType(), SourceRevision: s.GetSourceRevision(), SourceDigest: domain.Digest(s.GetSourceDigest()),
		PackageName: s.GetPackageName(), Version: s.GetVersion(), Npm: digestRef(s.GetNpm()), Browser: digestRef(s.GetBrowser()),
		BuildProfileID: s.GetBuildProfileId(), BuildProfileDigest: domain.Digest(s.GetBuildProfileDigest()), ValidatorProfileID: s.GetValidatorProfileId(),
		ValidatorProfileDigest: domain.Digest(s.GetValidatorProfileDigest()), HostAbi: s.GetHostAbi(), HostAbiDigest: domain.Digest(s.GetHostAbiDigest()),
		Destinations:                domain.ReleaseDestinations{NpmRegistry: s.GetNpmRegistry(), BrowserOrigin: s.GetBrowserOrigin()},
		CertificationEvidenceDigest: domain.Digest(s.GetCertificationEvidenceDigest()), SubjectDigest: domain.Digest(s.GetSubjectDigest()),
	}
	for _, c := range s.GetCss() {
		out.CSS = append(out.CSS, digestRef(c))
	}
	return out
}

func toSubject(s *domain.ReleaseSubject) *controlv1.ReleaseSubject {
	if s == nil {
		return nil
	}
	out := &controlv1.ReleaseSubject{
		ComponentId: s.ComponentID, PuckType: s.PuckType, SourceRevision: s.SourceRevision, SourceDigest: string(s.SourceDigest), PackageName: s.PackageName,
		Version: s.Version, Npm: toDigestRef(s.Npm), Browser: toDigestRef(s.Browser), BuildProfileId: s.BuildProfileID, BuildProfileDigest: string(s.BuildProfileDigest),
		ValidatorProfileId: s.ValidatorProfileID, ValidatorProfileDigest: string(s.ValidatorProfileDigest), HostAbi: s.HostAbi, HostAbiDigest: string(s.HostAbiDigest),
		NpmRegistry: s.Destinations.NpmRegistry, BrowserOrigin: s.Destinations.BrowserOrigin, CertificationEvidenceDigest: string(s.CertificationEvidenceDigest),
		SubjectDigest: string(s.SubjectDigest),
	}
	for _, c := range s.CSS {
		out.Css = append(out.Css, toDigestRef(c))
	}
	return out
}

func targetOf(t *controlv1.ReleaseTarget) domain.ReleaseTarget {
	return domain.ReleaseTarget{
		State: reverse(targetStateToProto, t.GetState()), EffectID: t.GetEffectId(), ReceiptID: t.GetReceiptId(), ReceiptDigest: t.GetReceiptDigest(),
		SubjectDigest: domain.Digest(t.GetSubjectDigest()), Destination: t.GetDestination(), Version: t.GetVersion(), ManifestDigest: t.GetManifestDigest(),
		FailureCode: t.GetFailureCode(),
	}
}

func toTarget(t domain.ReleaseTarget) *controlv1.ReleaseTarget {
	return &controlv1.ReleaseTarget{
		State: targetStateToProto[t.State], EffectId: optString(t.EffectID), ReceiptId: optString(t.ReceiptID), ReceiptDigest: optString(t.ReceiptDigest),
		SubjectDigest: optString(string(t.SubjectDigest)), Destination: optString(t.Destination), Version: optString(t.Version),
		ManifestDigest: optString(t.ManifestDigest), FailureCode: optString(t.FailureCode),
	}
}

func toRelease(p *domain.Release) *controlv1.Release {
	out := &controlv1.Release{
		OperationId: p.OperationID, TenantId: p.TenantID, Lineage: string(p.Lineage), SourceRevision: p.SourceRevision, State: releaseStateToProto[p.State],
		Subject: toSubject(p.Subject), ReleaseId: optString(p.ReleaseID), ReviewEffectId: optString(p.ReviewEffectID), ApprovalDeadline: optTime(p.ApprovalDeadline),
		Npm: toTarget(p.Npm), Browser: toTarget(p.Browser), Activation: toTarget(p.Activation), CatalogRevision: optString(p.CatalogRevision),
		FailureCode: optString(p.FailureCode), Revision: strconv.FormatUint(p.Revision, 10), UpdatedAt: timestamppb.New(p.UpdatedAt),
	}
	if a := p.Approval; a != nil {
		out.Approval = &controlv1.Approval{State: approvalStateToProto[a.State], SubjectDigest: string(a.SubjectDigest), ApproverId: optString(a.ApproverID),
			DecidedAt: optTime(a.DecidedAt), ReasonCode: optString(a.ReasonCode)}
	}
	return out
}

func (s *releaseServer) RecordRelease(ctx context.Context, req *controlv1.RecordReleaseRequest) (*controlv1.RecordReleaseResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	expected, err := strconv.ParseUint(req.GetExpectedRevision(), 10, 64)
	if err != nil {
		return nil, toStatus(fmt.Errorf("%w: expected revision", domain.ErrInvalid))
	}
	rec := domain.ReleaseRecord{
		ExpectedRevision: expected, State: reverse(releaseStateToProto, req.GetState()), Subject: subjectOf(req.GetSubject()), ReleaseID: req.GetReleaseId(),
		ReviewEffectID: req.GetReviewEffectId(), Npm: targetOf(req.GetNpm()), Browser: targetOf(req.GetBrowser()), Activation: targetOf(req.GetActivation()),
		CatalogRevision: req.GetCatalogRevision(), FailureCode: req.GetFailureCode(),
	}
	if req.ApprovalDeadline != nil {
		t := req.GetApprovalDeadline().AsTime()
		rec.ApprovalDeadline = &t
	}
	if a := req.GetApproval(); a != nil {
		rec.Approval = &domain.Approval{State: reverse(approvalStateToProto, a.GetState()), SubjectDigest: domain.Digest(a.GetSubjectDigest()), ApproverID: a.GetApproverId(), ReasonCode: a.GetReasonCode()}
		if a.DecidedAt != nil {
			t := a.GetDecidedAt().AsTime()
			rec.Approval.DecidedAt = &t
		}
	}
	p, existing, err := s.releases.Record(ctx, cmd, req.GetOperationId(), rec)
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.RecordReleaseResponse{Release: toRelease(p), Existing: existing}, nil
}

func (s *releaseServer) GetRelease(ctx context.Context, req *controlv1.GetReleaseRequest) (*controlv1.GetReleaseResponse, error) {
	p, err := s.releases.Get(ctx, req.GetTenantId(), req.GetOperationId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.GetReleaseResponse{Release: toRelease(p)}, nil
}
