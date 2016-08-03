DROP TABLE IF EXISTS mvp_hc__ProfessionalClaim ;

CREATE TABLE mvp_hc__ProfessionalClaim (
  mvp_hc__adjustedRepricedClaimNumber string,
  mvp_hc__allowedAmount integer,
  mvp_hc__ambulanceTransportReasonCode string,
  mvp_hc__approvedDRGAmount integer,
  mvp_hc__approvedDRGCode string,
  mvp_hc__approvedHCPCS string,
  mvp_hc__approvedQuantity integer,
  mvp_hc__claimFrequencyCode string,
  mvp_hc__claimNumber string,
  mvp_hc__claimType string,
  mvp_hc__icdTypeCode string,
  mvp_hc__medicalRecordNumber string,
  mvp_hc__otherDiagnosisCode10 string,
  mvp_hc__otherDiagnosisCode11 string,
  mvp_hc__otherDiagnosisCode12 string,
  mvp_hc__otherDiagnosisCode2 string,
  mvp_hc__otherDiagnosisCode3 string,
  mvp_hc__otherDiagnosisCode4 string,
  mvp_hc__otherDiagnosisCode5 string,
  mvp_hc__otherDiagnosisCode6 string,
  mvp_hc__otherDiagnosisCode7 string,
  mvp_hc__otherDiagnosisCode8 string,
  mvp_hc__otherDiagnosisCode9 string,
  mvp_hc__patientAmountPaid integer,
  mvp_hc__patientConditionCode string,
  mvp_hc__placeOfServiceCode string,
  mvp_hc__pricingMethodology string,
  mvp_hc__principalDiagnosisCode string,
  mvp_hc__priorAuthorizationNumber string,
  mvp_hc__referralNumber string,
  mvp_hc__rejectReasonCode string,
  mvp_hc__repricedClaimNumber string,
  mvp_hc__submitterClaimId string,
  mvp_hc__totalClaimChargeAmount integer,
  mvp_hc__unitMeasurementCode string,
  rdv_core__domainKey string,
  rdv_core__key bigint,
  mvp_hc__ambulancePickupLocation bigint,
  mvp_hc__otherReferringProvider bigint,
  mvp_hc__referringProvider bigint,
  mvp_hc__renderingProvider bigint,
  mvp_hc__serviceFacilityLocation bigint,
  mvp_hc__supervisingProvider bigint,
  mvp_hc__otherSubscriberInformation string,
  rdf_types string
)
USING column
OPTIONS(
  PERSISTENT ""
);