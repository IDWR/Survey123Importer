from dataclasses import dataclass
import datetime
import MeasurementDatabaseClient
import json
import logging.config

import MeasurementDatabaseClient.WaterDistrictDataService
import MeasurementDatabaseClient.repositories
from Survey123Client import Survey123Client


def main():
    logging.basicConfig(
        filename=".\\logs\\BasicSurvey123DataImport.log",
        level=logging.DEBUG,
        format='[%(asctime)s] - %(message)s',
        datefmt='%H:%M:%S'
    )
    logger = logging.getLogger("BasicSurvey123DataImport")

    try:
        config = json.load(open("config.json", "r"))

        # create logger
        logging.config.dictConfig(config["Logging"])
        # noinspection PyShadowingNames
        logger = logging.getLogger('Survey123DataImport')

        load_logger = SurveyLoadLogger(logger)

        conn_string = config["ConnectionStrings"]["MeasurementDatabaseClient"]
        pd_repository = MeasurementDatabaseClient.repositories.WdHydrologyPdRepository(conn_string)

        survey_dict = config["Surveys"]
        host_dict = config["SurveyHosts"]

        for district_number, survey_info in survey_dict.items():
            logger.info("Processing '{}' survey".format(district_number))
            host_info = host_dict[survey_info["host"]]
            client = Survey123Client(
                url=host_info["url"],
                username=host_info["username"],
                password=host_info["password"]
            )
            data_service = MeasurementDatabaseClient.WaterDistrictDataService.WaterDistrictDataService(conn_string)

            cutoff_date = data_service.get_earliest_date_of_last_measurement_for_water_district(district_number)
            where_clause = cutoff_date.strftime("\"DateOfVisit\" > DATE '%Y-%m-%d'")

            survey_returns = client.retrieve_survey_results(
                survey_id=survey_info["id"],
                field_dict=survey_info["fields"],
                where_clause=where_clause
            )

            records_to_be_imported = []

            for object_id, r in survey_returns.items():
                related_pd = pd_repository.get_by_location_id(r["SpatialDataID"])
                if related_pd is None:
                    continue
                data = MeasurementDatabaseClient.WdWaterMasterData(
                        WdHydrologyPdId=related_pd.ID,
                        HydrologyId=related_pd.HydrologyId,
                        MeasurementTypeId=r["MeasurementTypeId"] or 4,
                        Discharge=r["Discharge"],
                        DiversionDate=datetime.datetime.fromtimestamp(r["DiversionDate"] / 1e3).date(),
                        UserId=r["UserId"],
                        RegistrationId='45D3E06E-AAB9-46CD-A799-49096572F48D'
                    )
                device_type = MeasurementDatabaseClient.DeviceType.parse(r["DeviceType"])
                records_to_be_imported.append(MeasurementDatabaseClient.WdWaterMasterDataMetadata(data, device_type))

            data_service.import_measurements(records_to_be_imported)

            load_logger.add_result(SurveyLoadResult(
                district_number,
                data_service.Successes,
                data_service.DuplicateRows,
                [InvalidRow(r.ID, r.Message) for r in data_service.InvalidRows],
                data_service.TotalMeasurements,
                data_service.Interpolations))

        load_logger.finalize()

    except Exception as e:
        logger.exception(msg="Unhandled exception in Survey123DataImport", exc_info=e)


def guess_at_device_type(hydro_id: int):
    measurement_type_dict = {
        118387: MeasurementDatabaseClient.DeviceType.OpenChannel,
        119103: MeasurementDatabaseClient.DeviceType.ClosedConduit,
        119172: MeasurementDatabaseClient.DeviceType.OpenChannel,
        119198: MeasurementDatabaseClient.DeviceType.OpenChannel,
        119609: MeasurementDatabaseClient.DeviceType.OpenChannel,
        119752: MeasurementDatabaseClient.DeviceType.OpenChannel,
        120015: MeasurementDatabaseClient.DeviceType.OpenChannel,
        120865: MeasurementDatabaseClient.DeviceType.ClosedConduit,
        120879: MeasurementDatabaseClient.DeviceType.OpenChannel,
        490936: MeasurementDatabaseClient.DeviceType.ClosedConduit,
        524616: MeasurementDatabaseClient.DeviceType.OpenChannel,
        524617: MeasurementDatabaseClient.DeviceType.ClosedConduit,
        496337: MeasurementDatabaseClient.DeviceType.OpenChannel
    }
    return measurement_type_dict[hydro_id]


@dataclass
class SurveyLoadResult:
    """Object representing the result of loading the data from one survey into its target database"""
    ID: str
    SuccessCount: int
    DuplicateCount: int
    InvalidRows: list
    TotalMeasurements: int
    TotalInterpolations: int


@dataclass
class InvalidRow:
    """Object representing a summary of one row found to be invalid while loading a database"""
    HydrologyID: str
    Message: str


class SurveyLoadLogger:
    """
    Class that manages the logging of successes and failures of loading data from Survey123 into some other back end
    """
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self._results = []
        self.logger.info("Began logging")

    def add_result(self, result: SurveyLoadResult):
        """
        Adds the result of a load into the logger
        :param result: SurveyLoadResult object containing the results of a load
        :return: Nothing
        """
        self._results.append(result)
        self.logger.info("   - {} Successes".format(result.SuccessCount))
        self.logger.info("   - {} Duplicates".format(result.DuplicateCount))
        self.logger.info("   - {} Invalid Rows".format(len(result.InvalidRows)))
        self.logger.info("   - {} Total Measurements".format(result.TotalMeasurements))
        self.logger.info("   - {} Total Interpolations".format(result.TotalInterpolations))

    def finalize(self):
        """
        Finalize the log.  Wrap things up with a bow and close out the log file.
        :return:
        """
        results_with_invalid_values = {r.ID: r.InvalidRows for r in self._results if len(r.InvalidRows) > 0}
        if len(results_with_invalid_values) > 0:
            invalid_rows_message = ""
            for survey_id, invalid_rows in results_with_invalid_values.items():
                invalid_rows_message = invalid_rows_message + "\n\nSURVEY ID: {}".format(survey_id)
                for invalid_row in invalid_rows:
                    invalid_rows_message = invalid_rows_message + "\n   -- HydrologyID: {} -- {}".\
                        format(invalid_row.HydrologyID, invalid_row.Message)
                invalid_rows_message = invalid_rows_message + "\n"

            self.logger.error(invalid_rows_message)
        self.logger.info("Ended logging")


if __name__ == '__main__':
    main()
