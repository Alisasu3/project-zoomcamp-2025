terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.25.0"
    }
  }
}

provider "google" {
  credentials = "./keys/mycreds.json"
  project     = "projectzoomcamp2025"
  region      = "us-central1"
}

resource "google_storage_bucket" "dataset-zoomcampproject2025" {
  name          = "dataset-zoomcampproject2025"
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}