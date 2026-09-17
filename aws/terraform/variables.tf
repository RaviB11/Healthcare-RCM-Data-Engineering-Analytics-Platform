variable "region" {
  type        = string
  default     = "us-east-1"
  description = "AWS region for all platform resources"
}

variable "environment" {
  type        = string
  description = "Deployment environment"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "glue_jobs" {
  description = "Per-layer Glue job sizing. Gold does the heaviest shuffles."
  type = map(object({
    worker_type     = string
    workers         = number
    timeout_minutes = number
  }))
  default = {
    bronze-ingest = { worker_type = "G.1X", workers = 4, timeout_minutes = 60 }
    silver-build  = { worker_type = "G.2X", workers = 8, timeout_minutes = 120 }
    gold-build    = { worker_type = "G.2X", workers = 10, timeout_minutes = 120 }
  }
}

variable "redshift_base_capacity_rpu" {
  type        = number
  default     = 32
  description = "Redshift Serverless base RPUs. 8 is the floor; 32 suits this workload."
}

variable "alert_email" {
  type        = string
  description = "Address subscribed to the pipeline alert topic"
}
