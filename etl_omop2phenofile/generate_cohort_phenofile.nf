nextflow.enable.dsl = 2

log.info """\

================================================================================
ETL OMOP TO PHENOFILE PIPELINE
================================================================================
- lifebitai/etl_omop2phenofile
- docs: https://github.com/lifebit-ai/etl_omop2phenofile

Max memory                              : ${params.max_memory}
Max cpus                                : ${params.max_cpus}
Max time per job                        : ${params.max_time}
Output dir                              : ${params.outdir}
Launch dir                              : ${workflow.launchDir}
Working dir                             : ${workflow.workDir}
Script dir                              : ${workflow.projectDir}
User                                    : ${workflow.userName}

phenofileName                           : ${params.phenofileName}

covariateSpecifications                 : ${params.covariateSpecifications}
cohortSpecifications                    : ${params.cohortSpecifications}
codelistSpecifications                  : ${params.cohortSpecifications}
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

// Help message
def helpMessage() {
    log.info """
    Usage:
    The typical command for running the pipeline is as follows:
    nextflow run main.nf \
    --covariateSpecifications 'covariate_specs.json' \
    --cohortSpecifications 'user_cohort_specs.json' \
    --database_name "omop_data" \
    --database_host "localhost" \
    --database_port 5432 \
    --database_username "username" \
    --database_password "pass"

    Essential parameters:
    --covariateSpecifications   A file containing details of the covariates to include in the phenofile
    --cohortSpecifications      A file containing user-made cohort(s) specification

    Database parameters:
    --database_name             A database name
    --database_host             A database host
    --database_port             A database port
    --database_username         A database user name
    --database_password         A database password
    --database_dbms             A database dbms
    --database_cdm_schema       A database cdm schema
    --database_cohort_schema    A database cohort schema

    See docs/README.md for more details.
    """.stripIndent()
}

// Show help message
if (params.help) {
  helpMessage()
  exit 0
}

/*
* Processes without sub-workflows
*/
//Create introspection report
process obtain_pipeline_metadata {
  publishDir "${params.tracedir}", mode: "copy"

  input:
  val(repository)
  val(commit)
  val(revision)
  val(script_name)
  val(script_file)
  val(project_dir)
  val(launch_dir)
  val(work_dir)
  val(user_name)
  val(command_line)
  val(config_files)
  val(profile)
  val(container)
  val(container_engine)
  val(raci_owner)
  val(domain_keywords)

  output:
  path("pipeline_metadata_report.tsv")

  shell:
  '''
  echo "Repository\t!{repository}"                  > temp_report.tsv
  echo "Commit\t!{commit}"                         >> temp_report.tsv
  echo "Revision\t!{revision}"                     >> temp_report.tsv
  echo "Script name\t!{script_name}"               >> temp_report.tsv
  echo "Script file\t!{script_file}"               >> temp_report.tsv
  echo "Project directory\t!{project_dir}"         >> temp_report.tsv
  echo "Launch directory\t!{launch_dir}"           >> temp_report.tsv
  echo "Work directory\t!{work_dir}"               >> temp_report.tsv
  echo "User name\t!{user_name}"                   >> temp_report.tsv
  echo "Command line\t!{command_line}"             >> temp_report.tsv
  echo "Configuration file(s)\t!{config_files}"    >> temp_report.tsv
  echo "Profile\t!{profile}"                       >> temp_report.tsv
  echo "Container\t!{container}"                   >> temp_report.tsv
  echo "Container engine\t!{container_engine}"     >> temp_report.tsv
  echo "RACI owner\t!{raci_owner}"                 >> temp_report.tsv
  echo "Domain keywords\t!{domain_keywords}"       >> temp_report.tsv

  awk 'BEGIN{print "Metadata_variable\tValue"}{print}' OFS="\t" temp_report.tsv > pipeline_metadata_report.tsv
  '''
}

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
  each path("simpleCohortSpecFromCsv.R")
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
  each path("createCohortJsonFromSpec.R")
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
  each path("generateCohorts.R")
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
  each path("generatePhenofile.R")
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
  path("*phe"), emit: phenofile

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
    codelist_script
    codelist
    db_jars_for_codelist
    sqlite_db_cohorts
    concept_type
    domain
    control_group_occurrence
    cohort_json_from_spec_script
    cohort_specification_for_json
    generate_cohorts_script
    generate_covariates_script
    covariate_specification
    pheno_label
    convert_plink
    phenofile_name

  main:
    // get connection details
    retrieve_parameters()
    if (params.codelistSpecifications) {
      // generate cohort specs
      generate_user_spec_from_codelist(
        codelist_script,
        codelist,
        db_jars_for_codelist,
        retrieve_parameters.out.connection_details,
        sqlite_db_cohorts,
        concept_type,
        domain,
        control_group_occurrence
      )
      cohort_specification_for_json = generate_user_spec_from_codelist.out.cohort_specification_for_cohorts
    }
    // Obtain a OHDSI JSON cohort definition using a user-made input JSON specification file
    generate_cohort_jsons_from_user_spec(
      cohort_json_from_spec_script,
      cohort_specification_for_json,
      db_jars_for_codelist,
      retrieve_parameters.out.connection_details,
      sqlite_db_cohorts
    )
    // Using the cohort definition file(s), write cohort(s) in the OMOP database
    generate_cohorts_in_db(
      generate_cohorts_script,
      retrieve_parameters.out.connection_details,
      generate_cohort_jsons_from_user_spec.out.cohort_json_for_cohorts.collect(),
      cohort_specification_for_json,
      db_jars_for_codelist,
      sqlite_db_cohorts
    )
    // Generate a phenofile using the cohort(s) written to the OMOP database and
    // an input covariate specification
    generate_phenofile(
      generate_covariates_script,
      retrieve_parameters.out.connection_details,
      generate_cohorts_in_db.out.cohort_table_name,
      covariate_specification,
      generate_cohorts_in_db.out.cohort_counts,
      db_jars_for_codelist,
      generate_cohorts_in_db.out.sqlite_db_covariates,
      pheno_label,
      convert_plink,
      phenofile_name
    )
  
  emit:
    phenofile = generate_phenofile.out.phenofile

}

// main logic of the pipeline
workflow {

  // setting up introspection variables and channels
  // Importantly, in order to successfully introspect:
  // - This needs to be done first `main.nf`, before any (non-head) nodes are launched.
  // - All variables to be put into channels in order for them to be available later in `main.nf`.
  repository         = Channel.of(workflow.manifest.homePage)
  commitId           = Channel.of(workflow.commitId ?: "Not available is this execution mode. Please run 'nextflow run ${workflow.manifest.homePage} [...]' instead of 'nextflow run main.nf [...]'")
  revision           = Channel.of(workflow.manifest.version)
  scriptName         = Channel.of(workflow.scriptName)
  scriptFile         = Channel.of(workflow.scriptFile)
  projectDir         = Channel.of(workflow.projectDir)
  launchDir          = Channel.of(workflow.launchDir)
  workDir            = Channel.of(workflow.workDir)
  userName           = Channel.of(workflow.userName)
  commandLine        = Channel.of(workflow.commandLine)
  configFiles        = Channel.of(workflow.configFiles)
  profile            = Channel.of(workflow.profile)
  container          = Channel.of(workflow.container)
  containerEngine    = Channel.of(workflow.containerEngine)
  raci_owner         = Channel.of(params.raci_owner)
  domain_keywords    = Channel.of(params.domain_keywords)
  projectDir         = workflow.projectDir

  // Ensuring essential parameters are supplied
  if (!params.covariateSpecifications) {
    exit 1, "You have not supplied a file containing details of the covariates to include in the phenofile.\
    \nPlease use --covariateSpecifications."
  }
  if (!params.cohortSpecifications & !params.codelistSpecifications) {
    exit 1, "You have not supplied a file containing user-made cohort(s) specification or a codelist.\
    \nPlease use --cohortSpecifications or --codelistSpecifications."
  }
  if (!!params.cohortSpecifications & !!params.codelistSpecifications) {
    exit 1, "Choose either a cohort specifaction or a codelist specfication."
  }
  if (!!params.codelistSpecifications & !(!!params.conceptType & !!params.domain & !!params.controlIndexDate)) {
    exit 1, "When using a codelist specfication you must also specity a conceptType, a domain, and an index date for controls."
  }
  if (!params.database_cdm_schema) {
    exit 1, "You have not supplied the database cdm schema name.\
    \nPlease use --database_cdm_schema."
  }
  if (!params.database_cohort_schema) {
    exit 1, "You have not supplied the database cohort schema name.\
    \nPlease use --database_cohort_schema."
  }

  // Setting up input channels
  if (!!params.cohortSpecifications) {
    cohort_specification_for_json = Channel.fromPath(params.cohortSpecifications)
  } else {
    cohort_specification_for_json = Channel.empty()
  }
  covariate_specification = Channel.fromPath(params.covariateSpecifications)
  codelist = params.codelistSpecifications ? Channel.fromPath(params.codelistSpecifications) : Channel.empty()
  concept_type = params.codelistSpecifications ? Channel.value(params.conceptType) : Channel.empty()
  domain = params.codelistSpecifications ? Channel.value(params.domain) : Channel.empty()
  control_group_occurrence = params.codelistSpecifications ? Channel.value(params.controlIndexDate) : Channel.empty()
  db_jars = Channel
    .fromPath("${projectDir}/${params.path_to_db_jars}", type: 'file', followLinks: false)
  sqlite_db_cohorts = Channel.fromPath(params.sqlite_db)
  pheno_label = Channel.value(params.pheno_label)
  convert_plink = Channel.value(params.convert_plink)
  phenofile_name = Channel.value(params.phenofileName)

  // scripts
  codelist_script = Channel.fromPath("${projectDir}/bin/simpleCohortSpecFromCsv.R")
  cohort_json_from_spec_script = Channel.fromPath("${projectDir}/bin/createCohortJsonFromSpec.R")
  generate_cohorts_script = Channel.fromPath("${projectDir}/bin/generateCohorts.R")
  generate_covariates_script = Channel.fromPath("${projectDir}/bin/generatePhenofile.R")

  // sub-workflow
  lifebitai_generate_cohort_phenofile(
    codelist_script,
    codelist,
    db_jars,
    sqlite_db_cohorts,
    concept_type,
    domain,
    control_group_occurrence,
    cohort_json_from_spec_script,
    cohort_specification_for_json,
    generate_cohorts_script,
    generate_covariates_script,
    covariate_specification,
    pheno_label,
    convert_plink,
    phenofile_name
  )

  // metadata
  obtain_pipeline_metadata(
    repository,
    commitId,
    revision,
    scriptName,
    scriptFile,
    projectDir,
    launchDir,
    workDir,
    userName,
    commandLine,
    configFiles,
    profile,
    container,
    containerEngine,
    raci_owner,
    domain_keywords
  )

  userName = workflow.userName

  if ( userName == "ubuntu" || userName == "ec2-user") {
    workflow.onComplete {

    def trace_timestamp = new java.util.Date().format( 'yyyy-MM-dd_HH-mm-ss')

    traceReport = file("/home/${userName}/nf-out/trace.txt")
    traceReport.copyTo("results/pipeline_info/execution_trace_${trace_timestamp}.txt")
    }
  }
}
