// Public replay checks existing definitions; this command never supplies publisher authority.
import {readFileSync} from "node:fs";
import {join,resolve} from "node:path";
const [sources,revisionsPath]=process.argv.slice(2);
if(!sources||!revisionsPath)throw Error("Expected the exact app checkout root and revision file");
const revisions=JSON.parse(readFileSync(revisionsPath,"utf8"));
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
      const response=await fetch("https://axo709qs11.execute-api.us-east-1.amazonaws.com/v1/schemas",{
        method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({schema,mutation_mappers:{}}),
        signal:AbortSignal.timeout(60000)});
      const body=await response.json();
      const stored=body.schema??body;
      const match=response.status===200&&stored.owner_app_id===schema.owner_app_id
        &&stored.schema_type===schema.schema_type&&stored.key?.hash_field===schema.key.hash_field
        &&(stored.key?.range_field??null)===(schema.key.range_field??null)
        &&JSON.stringify([...(stored.fields??[])].sort())===JSON.stringify([...schema.fields].sort());
      console.log(JSON.stringify({app,schema:schema.descriptive_name,status:response.status,exact_definition:match}));
      if(!match)refused=true;
    }));
  }
}
if(refused)throw Error("Publish or repair the exact app definitions with their registered owner before the latency comparison.");

