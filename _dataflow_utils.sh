#!/usr/bin/env bash

function cancel_dataflow_job {
  local job_id="$1"
  echo "Canceling job. Job ID: ${job_id}"
  gcloud dataflow jobs cancel "${JOB_ID}" --region="${DATAFLOW_REGION}"
  sleep 3
  while : ; do
      current_state=$(gcloud dataflow jobs describe "${job_id}" --region="${DATAFLOW_REGION}" --format 'get(currentState)')
      [[ ! "${current_state}" == "JOB_STATE_CANCELLING" ]] && break;
      echo "Waiting for job cancellation. Current state: ${current_state}. Sleep 5s"
      sleep 5
  done
}

function wait_for_job_start {
  local job_name="$1"
  local job_filter="$2"
  while : ; do
    [[ ! "$(gcloud dataflow jobs list --region="${DATAFLOW_REGION}" --format 'get(id)' --filter="${job_filter}")" == "" ]] && break;
    echo "Waiting for job start. Sleep 5s"
    sleep 5
  done
}
