// Package jobs adapts the jobs contract (contracts/jobs, embedded by the
// contracts module's jobschema package) to Control's ManifestValidator
// port: schema validation of a result manifest and the reviewed job
// profile a result claims to come from.
package jobs

import (
	"fmt"

	"github.com/ancyloce/anvilkit-agent-contracts/go/jobschema"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Contract is the jobs contract of this build.
type Contract struct{}

// ValidateResultManifest validates manifest bytes against
// urn:anvilkit:jobs:v1#/$defs/resultManifest.
func (Contract) ValidateResultManifest(m []byte) error {
	return jobschema.ValidateResultManifest(m)
}

// JobProfile reads the reviewed job profile as acceptance needs it; an id
// no reviewed profile carries is not found. A fixture profile's fixed
// result carries the reviewed digest and byte size together: a profile
// whose expected result the contract cannot read (a size that is not a
// decimal sequence) is unusable for acceptance rather than a result
// without a trusted size.
func (Contract) JobProfile(id string) (domain.JobProfile, error) {
	p, err := jobschema.ProfileByID(id)
	if err != nil {
		return domain.JobProfile{}, fmt.Errorf("%w: job profile %q: %v", domain.ErrNotFound, id, err)
	}
	out := domain.JobProfile{ID: p.ProfileID, JobKind: p.JobKind, CandidateCode: p.CandidateCode}
	if p.ExpectedResult != nil {
		digest, err := domain.ParseDigest(p.ExpectedResult.ResultDigest)
		if err != nil {
			return domain.JobProfile{}, fmt.Errorf("job profile %q expected result: %w", id, err)
		}
		size, err := domain.ParseRevision(p.ExpectedResult.ResultSizeBytes)
		if err != nil {
			return domain.JobProfile{}, fmt.Errorf("job profile %q expected result size %q: %w", id, p.ExpectedResult.ResultSizeBytes, err)
		}
		out.ExpectedResult = &domain.FixedResult{Digest: digest, SizeBytes: int64(size)}
	}
	return out, nil
}
