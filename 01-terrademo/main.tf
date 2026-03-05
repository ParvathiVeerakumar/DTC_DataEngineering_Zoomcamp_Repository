terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.17.0"
    }
  }
}

provider "google" {
  project = "wise-scene-485519-q2"
  region  = "us-central1"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "wise-scene-485519-q2-terra-bucket"
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}