variable "environment" {
  type = string
}

variable "layers" {
  type    = list(string)
  default = ["bronze", "silver", "gold"]
}
