from __future__ import annotations

"""
RawEvidence — raw file contents collected from the Pegasus submit directory.

All fields are optional. The ReAct DiagnosisAgent calls tools that parse
whichever fields are populated. The POST script populates all of them
(it runs on the submit host where every file is local). The AMQP path
populates only what it can reach via shared filesystem.
"""

from pydantic import BaseModel, Field


class InputFileStatus(BaseModel):
    """Validation result for one file declared in transfer_input_files."""
    path: str
    exists: bool
    size_bytes: int | None = None
    is_empty: bool = False
    error: str | None = None


class RawEvidence(BaseModel):
    # ── From pegasus-analyzer CLI ─────────────────────────────────────────────
    pegasus_analyzer_output: str | None = None   # stdout of pegasus-analyzer

    # ── From Pegasus submit directory: 00/00/ job files ──────────────────────
    stderr_content: str | None = None            # {job}_ID{n}.err  (full)
    stdout_content: str | None = None            # {job}_ID{n}.out  (kickstart XML)
    sub_file_content: str | None = None          # {job}_ID{n}.sub  (resource requests)
    event_log_content: str | None = None         # {job}_ID{n}.log  (HTCondor events)

    # ── From Pegasus submit directory: workflow-level files ───────────────────
    dagman_log_content: str | None = None        # *.dag.dagman.out
    workflow_log_content: str | None = None      # workflow.log

    # ── From condor_history CLI ───────────────────────────────────────────────
    condor_classads_raw: str | None = None       # JSON from condor_history -json

    # ── Transformation script (if executable is local and readable) ───────────
    transformation_script_content: str | None = None  # content of the executable
    transformation_script_path: str | None = None     # absolute resolved path

    # ── Input file validation (from transfer_input_files in .sub file) ────────
    input_validation: list[InputFileStatus] = Field(default_factory=list)

    # ── SLURM partition profile (populated on SLURM clusters only) ────────────
    # current_partition: name parsed from batch_queue / +remote_queue in the .sub file
    # partition_profile: list of dicts (PartitionInfo.raw + healer fields) for all
    #   accessible partitions; serialised as plain dicts to avoid importing
    #   scheduler types into the evidence model.
    current_partition: str | None = None
    partition_profile: list[dict] = Field(default_factory=list)

    @property
    def available_sources(self) -> list[str]:
        """List which sources have content — shown to the agent as seed context."""
        available = []
        if self.pegasus_analyzer_output:
            available.append("pegasus_analyzer")
        if self.stderr_content:
            available.append("stderr")
        if self.stdout_content:
            available.append("stdout_kickstart")
        if self.sub_file_content:
            available.append("submit_file")
        if self.event_log_content:
            available.append("event_log")
        if self.dagman_log_content:
            available.append("dagman_log")
        if self.workflow_log_content:
            available.append("workflow_log")
        if self.condor_classads_raw:
            available.append("condor_history")
        if self.transformation_script_content:
            available.append("transformation_script")
        if self.input_validation:
            missing = sum(1 for f in self.input_validation if not f.exists)
            if missing:
                available.append(f"input_validation({missing}_missing)")
            else:
                available.append("input_validation(all_present)")
        if self.partition_profile:
            available.append(f"partition_profile({len(self.partition_profile)}_partitions)")
        return available
