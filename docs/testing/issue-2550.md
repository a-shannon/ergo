# Same-ID signature variants in candidate selection

Companion reproduction for [Ergo issue #2550](https://github.com/ergoplatform/ergo/issues/2550), following the [reproduction table](https://github.com/ergoplatform/ergo/issues/2550#issuecomment-5660244262).

This branch is based on `54dc966d1737551418fd353685775922860e3139` and adds two test files plus this guide. Production code is unchanged:

- `CandidateWitnessIdentityReproSpec.scala`: valid and invalid signature variants sharing an ID, direct validation, candidate selection and actual mempool invalidation.
- `CandidateCollectorComparison.scala`: a copy of the pinned collector and an experimental accepted-ID guard. The fixture checks the unchanged copy against production on every baseline input.

Use JDK 8 and the repository's sbt launcher. From the repository root on this branch:

```text
sbt -Denv=test -Dergo.directory=target/issue-2550-runtime -Drepro.output=target/issue-2550-output "++2.12.20" "set Test / fork := false" "set Test / parallelExecution := false" "set Test / testGrouping := Seq(Tests.Group(name.value, (Test / definedTests).value, Tests.InProcess))" "Test / testOnly org.ergoplatform.mining.CandidateWitnessIdentityReproSpec"
```

The successful recorded execution passed one property containing 18 matrix cases and six baseline-copy comparisons. Actual context: launch parameters, height 1, block version 1, sigma-state 6.0.3. The requested upcoming-version argument does not make this a block-version-4 test.

For `[I, V]`, unmodified production selects valid `V` but also returns its ID for elimination. First-ID filtering loses `V`; the accepted-ID-only experiment selects `V` but retains the earlier elimination entry. Applying those IDs to the mempool removes `V` in all three cases. Controls separately verify the differing script-validation outcomes and unchanged funding UTXO.

These assertions describe the reproduced defect. After implementing a correction, update the expected selection and invalidation outcomes accordingly; a passing reproduction is not evidence of a fix. Candidate-conflict policy, actor delivery, HTTP and live-chain behavior are outside this fixture.

The test writes its matrix and synthetic transaction/box fixtures under `target/issue-2550-output`. Use a fresh test output/runtime directory when repeating it. Only the two test sources and this guide belong to the contribution; generated output is not needed to apply the patch.
