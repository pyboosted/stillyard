//! Owner-visible exact inventory for audited retirement of a failed manager.
//! A preview or receipt is never a platform ProvenEmpty proof.
use super::*;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DomainClearanceInventory {
    pub machine_id: Uuid,
    pub authority_epoch: Uuid,
    pub installation: InstallationIdentity,
    pub manager_store_uuid: Uuid,
    pub session: Option<SessionIdentity>,
    pub committed: bool,
    pub grants: Vec<GrantSnapshot>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DomainClearancePreview {
    pub inventory: DomainClearanceInventory,
    pub sha256: String,
}
impl DomainClearancePreview {
    pub fn new(inventory: DomainClearanceInventory) -> std::io::Result<Self> {
        Ok(Self {
            sha256: payload_hash(&inventory)?,
            inventory,
        })
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DomainRetirementRequest {
    pub operation_id: Uuid,
    pub domain_id: crate::ExecutionDomainId,
    pub expected_inventory_sha256: String,
    pub reason: String,
    pub accept_risk: bool,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DomainRetirementReceipt {
    pub operation_id: Uuid,
    pub domain_id: crate::ExecutionDomainId,
    pub manager_store_uuid: Uuid,
    pub inventory_sha256: String,
    pub audit_sha256: String,
    pub requester: crate::ProcessIdentity,
    pub requester_principal: String,
    pub reason: String,
    pub risk_accepted: bool,
    pub retired_unix_millis: i64,
    pub grants_retired: u32,
    pub tickets_retired: u32,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DomainRetirementAudit {
    pub preview: DomainClearancePreview,
    pub request: DomainRetirementRequest,
    pub requester: crate::ProcessIdentity,
    pub requester_principal: String,
    pub retired_unix_millis: i64,
}
impl DomainRetirementAudit {
    pub fn receipt(&self) -> std::io::Result<DomainRetirementReceipt> {
        Ok(DomainRetirementReceipt {
            operation_id: self.request.operation_id,
            domain_id: self.request.domain_id,
            manager_store_uuid: self.preview.inventory.manager_store_uuid,
            inventory_sha256: self.preview.sha256.clone(),
            audit_sha256: payload_hash(self)?,
            requester: self.requester.clone(),
            requester_principal: self.requester_principal.clone(),
            reason: self.request.reason.clone(),
            risk_accepted: self.request.accept_risk,
            retired_unix_millis: self.retired_unix_millis,
            grants_retired: self
                .preview
                .inventory
                .grants
                .len()
                .try_into()
                .map_err(std::io::Error::other)?,
            tickets_retired: self
                .preview
                .inventory
                .grants
                .iter()
                .map(|g| g.tickets.len())
                .sum::<usize>()
                .try_into()
                .map_err(std::io::Error::other)?,
        })
    }
}
