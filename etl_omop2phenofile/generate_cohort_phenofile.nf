#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

log.info """\

================================================================================
ETL OMOP TO PHENOFILE PIPELINE
================================================================================
- lifebitai/etl_omop2phenofile
- docs: https://github.com/lifebit-ai/etl_omop2phenofile

Output dir                              : ${params.outdir}
Launch dir                              : ${workflow.launchDir}
Working dir                             : ${workflow.workDir}
Script dir                              : ${workflow.projectDir}
User                                    : ${workflow.userName}

phenofileName                           : ${params.phenofileName}

covariateSpecifications                 : ${params.covariateSpecifications}
domain                                  : ${params.domain}
conceptType                             : ${params.conceptType}
controlIndexDate                        : ${params.controlIndexDate}

sqlite_db                               : ${params.sqlite_db}
database_dbms                           : ${params.database_dbms}
database_cdm_schema                     : ${params.database_cdm_schema}
database_cohort_schema                  : ${params.database_cohort_schema}

aws_param_name_for_database_host        : ${params.aws_param_name_for_database_host}
aws_param_name_for_database_name        : ${params.aws_param_name_for_database_name}
aws_param_name_for_database_port        : ${params.aws_param_name_for_database_port}
aws_param_name_for_database_username    : ${params.aws_param_name_for_database_username}
database_name                           : ${params.database_name}
database_host                           : ${params.database_host}
database_port                           : ${params.database_port}
database_username                       : ${params.database_username}

-\033[2m--------------------------------------------------\033[0m-
"""

/*
* Processes part of the subworkflow 'generate_cohort_phenofile'
*/
process retrieve_parameters {

  output:
  path("*.log"), emit: retrieve_ssm_parameters_log
  path("*.json"), emit: connection_details

  shell:
  '''
  if [ !{params.param_via_aws} = true ]
  then
    database_host=\$(aws ssm get-parameter --name "!{params.aws_param_name_for_database_host}" --region !{params.aws_region} | jq -r ".Parameter.Value")
    database_port=\$(aws ssm get-parameter --name "!{params.aws_param_name_for_database_port}" --region !{params.aws_region} | jq -r ".Parameter.Value")
    database_username=\$(aws ssm get-parameter --name "!{params.aws_param_name_for_database_username}" --region !{params.aws_region} | jq -r ".Parameter.Value")
    database_password=\$(aws ssm get-parameter --name "!{params.aws_param_name_for_database_password}" --region !{params.aws_region} | jq -r ".Parameter.Value")
    database_name=\$(aws ssm get-parameter --name "!{params.aws_param_name_for_database_name}" --region !{params.aws_region} | jq -r ".Parameter.Value")
  else
    database_host="!{params.database_host}"
    database_port="!{params.database_port}"
    database_username="!{params.database_username}"
    database_password="!{params.database_password}"
    database_name="!{params.database_name}"
  fi

  string="{\n"
  string+='"dbms":"!{params.database_dbms}",\n'

  if [[ "\$database_name" != "false"  && "\$database_password" != "false" && "\$database_username" != "false" && "\$database_port" != "false" ]]
  then
    string=$(echo $string'"server":"'\$database_host'/'\$database_name'",\n')
    string=$(echo $string'"port":"'\$database_port'",\n')
    string=$(echo $string'"user":"'\$database_username'",\n')
    string=$(echo $string'"password":"'\$database_password'",\n')
  else
    string=$(echo $string'"server":"'\$database_host'",\n')
  fi

  string+='"cdmDatabaseSchema":"!{params.database_cdm_schema}",\n'
  string+='"cohortDatabaseSchema":"!{params.database_cohort_schema}"\n'
  string+="\n}"
  echo $string
  echo -e $string > connection_details.json

  echo "Database parameters were retrieved" > ssm_parameter_retrieval.log
  '''
}

process generate_user_spec_from_codelist {
  publishDir "${params.outdir}/cohorts/user_def", mode: "copy"

  input:
  each path(codelist)
  each path(db_jars)
  each path(connection_details)
  each path(sqlite_db)
  val(concept_type)
  val(domain)
  val(control_group_occurrence)

  output:
  path("*json"), emit: cohort_specification_for_cohorts

  shell:
  """
  ## Make a permanent copy of sqlite file (NB. This is only used in sqlite testing mode)
  ls -la
  mkdir omopdb/
  chmod 0766 ${sqlite_db}
  cp ${sqlite_db} omopdb/omopdb.sqlite
  mv omopdb/omopdb.sqlite .
  Rscript simpleCohortSpecFromCsv.R \
    --codelist=${codelist} \
    --connection_details=${connection_details} \
    --db_jars=${db_jars} \
    --concept_types=${concept_type} \
    --domain=${domain} \
    --control_group_occurrence=${control_group_occurrence}
  """
}

process generate_cohort_jsons_from_user_spec {
  publishDir "${params.outdir}/cohorts/json", mode: "copy"

  input:
  each path(spec)
  each path(db_jars)
  each path(connection_details)
  each path(sqlite_db)

  output:
  path("*json"), emit: cohort_json_for_cohorts

  shell:
  """
  ## Make a permanent copy of sqlite file (NB. This is only used in sqlite testing mode)
  mkdir omopdb/
  chmod 0766 ${sqlite_db}
  cp ${sqlite_db} omopdb/omopdb.sqlite
  mv omopdb/omopdb.sqlite .
  Rscript createCohortJsonFromSpec.R \
  --cohort_specs=${spec} \
  --connection_details=${connection_details} \
  --db_jars=${db_jars}
  """
}

process generate_cohorts_in_db {
  publishDir "${params.outdir}", mode: "copy",
   saveAs: { filename -> 
      if (filename.endsWith('csv')) "cohorts/$filename"
    }

  input:
  each path(connection_details)
  path("*")
  each path(spec)
  each path(db_jars)
  each path(sqlite_db)

  output:
  path("*txt"), emit: cohort_table_name
  path("*csv"), emit: cohort_counts
  path("omopdb.sqlite"), emit: sqlite_db_covariates

  shell:
  """
  ## Make a permanent copy of sqlite file (NB. This is only used in sqlite testing mode)
  mkdir omopdb/
  chmod 0766 ${sqlite_db}
  cp ${sqlite_db} omopdb/omopdb.sqlite
  mv omopdb/omopdb.sqlite .
  Rscript generateCohorts.R --connection_details=${connection_details} --db_jars=${db_jars} --cohort_specs=${spec}
  """
}

process generate_phenofile {
  publishDir "${params.outdir}/phenofile", mode: "copy"

  input:
  each path(connection_details)
  each path(cohort_table_name)
  each path(covariate_specs)
  each path(cohort_counts)
  each path(db_jars)
  each path(sqlite_db)
  val(pheno_label)
  val(convert_plink)
  val(phenofile_name)

  output:
  path("*phe")

  shell:
  """
  Rscript generatePhenofile.R \
    --connection_details=${connection_details} \
    --cohort_counts=${cohort_counts} \
    --cohort_table=${cohort_table_name} \
    --covariate_spec=${covariate_specs} \
    --db_jars=${db_jars} \
    --pheno_label=${pheno_label} \
    --convert_plink=${convert_plink} \
    --phenofile_name=${phenofile_name}
  """
}

workflow lifebitai_generate_cohort_phenofile {

  take:
    input_specifications
    type_specifications

  main:

    if (type_specifications != 'codelist') {
      exit 1, "You have not supplied a valid type of specification.\
      \nPlease use 'codelist' or 'cohort' only."
    }
    if (type_specifications != 'codelist' & !(!!params.conceptType & !!params.domain & !!params.controlIndexDate)) {
      exit 1, "When using a codelist specfication you must also specity a conceptType, a domain, and an index date for controls."
    }

    // Define Channels only needed inside the sub-workflow
    if(type_specifications == 'codelist'){
      concept_type = Channel.value(params.conceptType)
      domain = Channel.value(params.domain)
      control_group_occurrence = Channel.value(params.controlIndexDate)
    }
    covariate_specification = params.covariateSpecifications ? Channel.fromPath(params.covariateSpecifications) : Channel.empty()
    db_jars = Channel.fromPath("${projectDir}/${params.path_to_db_jars}", type: 'file', followLinks: false)
    sqlite_db_cohorts = Channel.fromPath(params.sqlite_db)
    pheno_label = Channel.value(params.pheno_label)
    convert_plink = Channel.value(params.convert_plink)
    phenofile_name = Channel.value(params.phenofileName)

    // get connection details
    retrieve_parameters()

    // generate cohort specs
    if (type_specifications == 'codelist') {
      generate_user_spec_from_codelist(
        input_specifications,
        db_jars,
        retrieve_parameters.out.connection_details,
        sqlite_db_cohorts,
        concept_type,
        domain,
        control_group_occurrence
      )
      cohort_specs = generate_user_spec_from_codelist.out.cohort_specification_for_cohorts
    }

    if (type_specifications == 'cohort') {
      cohort_specs = input_specifications
    }

    // Obtain a OHDSI JSON cohort definition using a user-made input JSON specification file
    generate_cohort_jsons_from_user_spec(
      cohort_specs,
      db_jars,
      retrieve_parameters.out.connection_details,
      sqlite_db_cohorts
    )

    // Using the cohort definition file(s), write cohort(s) in the OMOP database
    generate_cohorts_in_db(
      retrieve_parameters.out.connection_details,
      generate_cohort_jsons_from_user_spec.out.cohort_json_for_cohorts.collect(),
      cohort_specs,
      db_jars,
      sqlite_db_cohorts
    )

    // Generate a phenofile using the cohort(s) written to the OMOP database and
    // an input covariate specification
    generate_phenofile(
      retrieve_parameters.out.connection_details,
      generate_cohorts_in_db.out.cohort_table_name,
      covariate_specification,
      generate_cohorts_in_db.out.cohort_counts,
      db_jars,
      generate_cohorts_in_db.out.sqlite_db_covariates,
      pheno_label,
      convert_plink,
      phenofile_name
    )
  
  emit:
    generate_phenofile.out

}

// main logic of the pipeline
workflow {

  projectDir         = workflow.projectDir

  // Ensuring essential parameters are supplied
  if (!params.covariateSpecifications) {
    exit 1, "You have not supplied a file containing details of the covariates to include in the phenofile.\
    \nPlease use --covariateSpecifications."
  }
  if (!params.database_cdm_schema) {
    exit 1, "You have not supplied the database cdm schema name.\
    \nPlease use --database_cdm_schema."
  }
  if (!params.database_cohort_schema) {
    exit 1, "You have not supplied the database cohort schema name.\
    \nPlease use --database_cohort_schema."
  }

  input_specifications = Channel.fromPath(params.input_specifications, checkIfExists: true)
  type_specifications = Channel.value(params.type_specifications)
  println "before sub-workflow"

  // sub-workflow
  lifebitai_generate_cohort_phenofile(
    input_specifications,
    type_specifications
  )
}
