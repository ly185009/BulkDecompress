function transform(inJson) {
  var obj = JSON.parse(inJson);
  var pusbsub = {};
  pusbsub.attributes = []
  pusbsub.topic = ""
  pusbsub.organizationName = obj.tenantId;
  pusbsub.correlationId = obj.id;
  pusbsub.enterpriseUnitId = {};
  pusbsub.enterpriseUnitId.enterpriseUnitId = obj.enterpriseUnit.id
  pusbsub.payload = JSON.stringify(obj);
  return JSON.stringify(pusbsub);
}