import argparse
import os
import sys
from emr_ingestion.jobs.jobEnrichPerson import JobEnrichPerson
from emr_ingestion.jobs.jobDataHub import JobDataHub
from emr_ingestion.jobs.jobIngestLemitt import JobIngestLemitt
from emr_ingestion.jobs.jobIngestBigDataCorpTxt import jobIngestBigDataCorpTxt
from emr_ingestion.jobs.jobIngestSocio import JobIngestSocio
from emr_ingestion.jobs.jobExporterAliansce import JobExporterAliansce
from emr_ingestion.jobs.jobIngestPortalEstadual import JobIngestPortalEstadual
from emr_ingestion.jobs.jobIngestPortalFederal import JobIngestPortalFederal

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='EMR Ingestion jobs')
    parser.add_argument("--enrichPerson", help="process the enrich person process")
    parser.add_argument('-p', '--providerName', type=str, required=False, default=None,
                        help='Name of the provider to be executed')
    parser.add_argument("--enrichPersonPrep_Full", help="process the enrich person process prep full")
    parser.add_argument("--dataHub", help="process the ingestion of the Datahub")
    parser.add_argument("--dataHubFull", help="process the ingestion of the Datahub fully")
    parser.add_argument("--ingestLemitt", help="process the ingestion of the Lemit")
    parser.add_argument("--ingestLemittFull", help="process the ingestion of the Lemit fully")
    parser.add_argument("--ingestBigDataCorpTxt", help="process the ingestion of bigdata_corp txt files")
    parser.add_argument("--ingestBigDataCorpTxtFull", help="process the ingestion of bigdata_corp txt files fully")
    parser.add_argument("--ingestSocio", help="process the ingestion of the socio empresa")
    parser.add_argument("--ingestSocioFull", help="process the ingestion of the socio empresa fully")
    parser.add_argument("--exporterAliansce", help="process the Aliansce exporter")
    parser.add_argument("--exporterAliansce1", help="process the Aliansce exporter")
    parser.add_argument("--exporterAliansce2", help="process the Aliansce exporter")
    parser.add_argument("--exporterAliansce3", help="process the Aliansce exporter")
    parser.add_argument("--exporterAliansce4", help="process the Aliansce exporter")
    parser.add_argument("--exporterAliansce5", help="process the Aliansce exporter")
    parser.add_argument("--lastFile", help="Last modified file to be executed")
    parser.add_argument("--ingestPortalTranspEstadual", help="process the ingestion of the portal transparencia estadual")
    parser.add_argument("--ingestPortalTranspEstadualFull", help="process the ingestion of the portal transparencia estadual full")
    parser.add_argument("--ingestPortalTranspFederal", help="process the ingestion of the portal transparencia federal")
    parser.add_argument("--ingestPortalTranspFederalFull", help="process the ingestion of the portal transparencia federal full")
    

    args = parser.parse_args()

    pex_file = os.path.basename([path for path in sys.path if path.endswith('.pex')][0])
    os.environ['PYSPARK_PYTHON'] = "./" + pex_file

    if args.enrichPerson:
        provider_name = args.providerName
        jobPerson = JobEnrichPerson(pex_file, provider_name)
        jobPerson.execute()
    if args.enrichPersonPrep_Full:
        provider_name = args.providerName
        jobPerson = JobEnrichPerson(pex_file, provider_name, True)
        jobPerson.execute()
    if args.dataHub:
        provider_name = args.dataHub
        jobPerson = JobDataHub(pex_file)
        jobPerson.execute()
    if args.dataHubFull:
        provider_name = args.dataHubFull
        jobPerson = JobDataHub(pex_file, True)
        jobPerson.execute()
    if args.ingestLemitt:
        provider_name = args.ingestLemitt
        jobPerson = JobIngestLemitt(pex_file)
        jobPerson.execute()
    if args.ingestLemittFull:
        provider_name = args.ingestLemittFull
        jobPerson = JobIngestLemitt(pex_file, True)
        jobPerson.execute()
    if args.ingestBigDataCorpTxt:
        provider_name = args.ingestBigDataCorpTxt
        jobPerson = jobIngestBigDataCorpTxt(pex_file)
        jobPerson.execute()
    if args.ingestBigDataCorpTxtFull:
        provider_name = args.ingestBigDataCorpTxtFull
        jobPerson = jobIngestBigDataCorpTxt(pex_file, True)
        jobPerson.execute()
    if args.ingestSocio:
        provider_name = args.ingestSocio
        jobPerson = JobIngestSocio(pex_file)
        jobPerson.execute()
    if args.ingestSocioFull:
        provider_name = args.ingestSocioFull
        jobPerson = JobIngestSocio(pex_file, True)
        jobPerson.execute()
    if args.exporterAliansce:
        JobExporterAliansce = JobExporterAliansce(pex_file)
        JobExporterAliansce.execute()
    if args.exporterAliansce1:
        JobExporterAliansce = JobExporterAliansce(pex_file, 1, args.lastFile)
        JobExporterAliansce.execute()
    if args.exporterAliansce2:
        JobExporterAliansce = JobExporterAliansce(pex_file, 2, args.lastFile)
        JobExporterAliansce.execute()
    if args.exporterAliansce3:
        JobExporterAliansce = JobExporterAliansce(pex_file, 3, args.lastFile)
        JobExporterAliansce.execute()
    if args.exporterAliansce4:
        JobExporterAliansce = JobExporterAliansce(pex_file, 4, args.lastFile)
        JobExporterAliansce.execute()
    if args.exporterAliansce5:
        JobExporterAliansce = JobExporterAliansce(pex_file, 5, args.lastFile)
        JobExporterAliansce.execute()
    if args.ingestPortalTranspEstadual:
        provider_name = args.ingestPortalTranspEstadual
        jobPerson = JobIngestPortalEstadual(pex_file)
        jobPerson.execute()
    if args.ingestPortalTranspEstadualFull:
        provider_name = args.ingestPortalTranspEstadualFull
        jobPerson = JobIngestPortalEstadual(pex_file, True)
        jobPerson.execute()
    if args.ingestPortalTranspFederal:
        provider_name = args.ingestPortalTranspFederal
        jobPerson = JobIngestPortalFederal(pex_file)
        jobPerson.execute()
    if args.ingestPortalTranspFederalFull:
        provider_name = args.ingestPortalTranspFederalFull
        jobPerson = JobIngestPortalFederal(pex_file, True)
        jobPerson.execute()
    else:
        print("Pass the step do want to execute! Use -h parameter for help.")

