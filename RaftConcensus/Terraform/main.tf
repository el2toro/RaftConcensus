provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "rg" {
  name     = "raft-aks-rg"
  location = "eastus"
}

# AKS Cluster
resource "azurerm_kubernetes_cluster" "aks" {
  name                = "raft-aks"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  dns_prefix          = "raftaks"

  default_node_pool {
    name       = "default"
    node_count = 3  # 3 nodes for Raft quorum
    vm_size    = "Standard_B2s"  # Cost-effective, scalable
    zones      = ["1", "2", "3"]  # Multi-zone for HA
  }

  identity {
    type = "SystemAssigned"  # Managed Identity for Azure integrations
  }

  network_profile {
    network_plugin = "azure"  # Azure CNI for pod networking
  }
}

# Storage Account for Blob Backups
resource "azurerm_storage_account" "storage" {
  name                     = "raftaksstorage${random_string.unique.result}"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "GRS"  # Geo-redundant
}

resource "azurerm_storage_container" "container" {
  name                  = "raft-logs"
  storage_account_name  = azurerm_storage_account.storage.name
  container_access_type = "private"
}

# Role Assignment for AKS Managed Identity to Access Storage
resource "azurerm_role_assignment" "storage_access" {
  principal_id     = azurerm_kubernetes_cluster.aks.identity[0].principal_id
  role_definition_name = "Storage Blob Data Contributor"
  scope            = azurerm_storage_account.storage.id
}

output "kube_config" {
  value = azurerm_kubernetes_cluster.aks.kube_config_raw
  sensitive = true
}