// Public replay checks existing definitions; this command never supplies publisher authority.
// The registry is the one the comparison daemons use: `ENVIRONMENT` selects it from
// folddb_profile/environments.json, the same source the daemon reads at build time.
import {readFileSync} from "node:fs";
import {join,resolve} from "node:path";
const [sources,revisionsPath]=process.argv.slice(2);
if(!sources||!revisionsPath)throw Error("Expected the exact app checkout root and revision file");
const environment=process.env.ENVIRONMENT;
const registry=JSON.parse(readFileSync(new URL("../../folddb_profile/environments.json",import.meta.url),"utf8"));
const service=registry.environments[environment]?.schema_service;
if(!service)throw Error("Set ENVIRONMENT to the Schema Service environment the comparison daemons use");
const revisions=JSON.parse(readFileSync(revisionsPath,"utf8"));
const stable=(value)=>JSON.stringify(order(value));
function order(value) {
  if(Array.isArray(value))return value.map(order);
  if(value&&typeof value==="object")return Object.fromEntries(Object.keys(value).sort().map(key=>[key,order(value[key])]));
  return value;
}
let refused=false;
for(const app of Object.keys(revisions)) {
  const root=resolve(sources,app,"baseline");
  const definitions=await import(join(root,"src/schemas.ts"));
  const {canonicalSchema}=await import(join(root,"src/canonical-vocabulary.ts"));
  const entries=app==="fkanban"?[...definitions.UNIQUE_SCHEMAS,...definitions.EXTRA_SCHEMAS]:definitions.UNIQUE_SCHEMAS;
  for(let start=0;start<entries.length;start+=4) {
    await Promise.all(entries.slice(start,start+4).map(async(entry)=>{
      const schema=canonicalSchema(entry.schema.schema);
      // Schema Service's cert-free repost route returns only an existing definition.
      // A new definition must stop at the normal publisher gate.
      const response=await fetch(`${service}/v1/schemas`,{
        method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({schema,mutation_mappers:{}}),
        signal:AbortSignal.timeout(60000)});
      const body=await response.json();
      const stored=body.schema??body;
      // The service stores an absent key part as null; the app source omits it.
      const match=response.status===200&&stored.owner_app_id===schema.owner_app_id
        &&stored.descriptive_name===schema.descriptive_name&&/^[a-f0-9]{64}$/.test(stored.identity_hash??"")
        &&stored.schema_type===schema.schema_type
        &&(stored.key?.hash_field??null)===(schema.key?.hash_field??null)
        &&(stored.key?.range_field??null)===(schema.key?.range_field??null)
        &&stable([...(stored.fields??[])].sort())===stable([...schema.fields].sort())
        &&schema.fields.every(field=>stable(stored.field_types?.[field])===stable(schema.field_types[field])
          &&stored.field_descriptions?.[field]===schema.field_descriptions[field]);
      console.log(JSON.stringify({app,environment,schema:schema.descriptive_name,status:response.status,exact_definition:match}));
      if(!match)refused=true;
    }));
  }
}
if(refused)throw Error(`Publish or repair the exact app definitions in the ${environment} Schema Service before the latency comparison.`);
