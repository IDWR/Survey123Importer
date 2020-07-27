import datetime
from datetime import timedelta

from MeasurementDatabaseClient import WdWaterMasterData, DeviceType, MeasurementDatabaseInvalidData
from MeasurementDatabaseClient.exceptions import AlreadyGotOneException, InvalidDataException
from MeasurementDatabaseClient.repositories import WdHydrologyPdRepository, WdWaterMasterDataRepository


class WaterDistrictDataService(object):
    """
    Service that coordinates actions between WdWaterMasterData and WdHydrologyPd Repositories
    """
    def __init__(self, connection_string: str):
        self.__connection_string = connection_string
        self.__data_repo = WdWaterMasterDataRepository(self.__connection_string)
        self.__hydro_pd_repo = WdHydrologyPdRepository(self.__connection_string)
        self.Interpolations = 0
        self.Measurements = 0
        self.Successes = 0
        self.DuplicateRows = 0
        self.InvalidRows = []

    def add_measurement(self, measurement):
        """
        Adds one measurement to the WdWaterMasterData table
        :param measurement: measurement to be added
        :return: Not a darn thing
        """
        if measurement.MeasurementTypeId == 3:
            self.Interpolations += 1
        else:
            self.TotalMeasurements += 1
        try:
            self.__data_repo.add_measurement(measurement)
            self.Successes += 1
        except InvalidDataException as e:
            self.InvalidRows.append(
                MeasurementDatabaseInvalidData(measurement.HydrologyId,
                                               "Invalid fields: {}".format(','.join(e.field_list))))
        except AlreadyGotOneException:
            self.DuplicateRows += 1

    def import_measurements(self, measurements: list):
        """
        Adds a list of data to the database and performs necessary processing such as interpolation for missing days
        :type measurements: list of WdWaterMasterDataMetadata
        """
        self.__reset_tracker()
        self.__data_repo = WdWaterMasterDataRepository(self.__connection_string)
        interpolated_rows = []
        measurements_by_id = self.__sort_measurements_by_hydro_id(measurements)
        for each_measurement_list in measurements_by_id:
            previous_record = None
            for this_record in each_measurement_list:
                data = this_record.Data
                if previous_record is None:
                    previous_record = self.__data_repo.get_last_measurement_at_hydro_id(data.HydrologyId)
                if previous_record is not None:
                    interpolated_rows.extend(self.__interpolate_data(previous_record, data, this_record.DeviceType))
                previous_record = data
        for measurement in measurements:
            self.add_measurement(measurement.Data)
        for measurement in interpolated_rows:
            self.add_measurement(measurement)
        self.__data_repo.complete()
        self.__data_repo.close()

    def get_earliest_date_of_last_measurement_for_water_district(self, district_number: str):
        """
        For all of a particular district's diversions, this will return the last date of measurement
        for the diversion that was measured last LONGEST ago.  If no measurements are found then 01 Jan of the current
        year will be returned
        """
        hydro_pds = self.__hydro_pd_repo.get_by_water_district(district_number)
        today = datetime.date.today()
        return_date = today
        for pd in hydro_pds:
            last_measurement_at_hydro_id = self.__data_repo.get_last_measurement_at_hydro_id(pd.HydrologyId)
            if last_measurement_at_hydro_id is None:
                return_date = datetime.datetime(today.year, 1, 1).date()
                continue
            return_date = min(last_measurement_at_hydro_id.DiversionDate, return_date)
        return return_date

    def __reset_tracker(self):
        """
        Resets the tracking elements of this class
        :return: Not a darn thing
        """
        self.TotalMeasurements = 0
        self.Successes = 0
        self.DuplicateRows = 0
        self.InvalidRows = []

    @staticmethod
    def __interpolate_data(start_data: WdWaterMasterData, end_data: WdWaterMasterData, device_type: DeviceType):
        """
        Creates interpolated daily WdWaterMasterData records to fill gaps between two measured WdWaterMasterData records
        :param start_data: Measured data at the beginning of a gap
        :param end_data: Measured data at the end of the gap
        :param device_type: Type of device used for measurement (informs how interpolation will be done)
        :return: list of interpolated WdWaterMasterData records
        """
        interpolated_data = []
        start_date = start_data.DiversionDate
        end_date = end_data.DiversionDate
        start_cfs = start_data.Discharge
        days_to_interpolate = (end_date - start_date).days
        if days_to_interpolate == 0:
            return []
        daily_cfs_step = 0
        dt_cc = DeviceType.ClosedConduit
        if device_type == dt_cc:
            cfs_difference = end_data.Discharge - start_cfs
            daily_cfs_step = cfs_difference / days_to_interpolate
        next_date = start_date
        next_cfs = start_cfs
        for d in range(days_to_interpolate):
            next_date = next_date + timedelta(days=1)
            next_cfs = next_cfs + daily_cfs_step
            data = WdWaterMasterData(
                WdHydrologyPdId=end_data.WdHydrologyPdId,
                HydrologyId=end_data.HydrologyId,
                DiversionDate=next_date,
                MeasurementTypeId=3,
                Discharge=next_cfs,
                RegistrationId=end_data.RegistrationId,
                UserId=end_data.UserId
            )
            interpolated_data.append(data)

        return interpolated_data

    @staticmethod
    def __sort_measurements_by_hydro_id(measurements: list):
        """
        Sorts a list of measurements for many HydrologyIDs into separate lists, one for each HydrologyID.
        Each list of measurements is ordered by ascending date.
        :param measurements: List of measurements
        :return: list of lists of measurements, each list for a particular HydrologyID
        """
        measurements_by_id = {}
        for each_measurement in measurements:
            # Sort list into dictionary keyed by PdId, each member is a list of measurements ordered by date
            hydro_id = each_measurement.Data.HydrologyId
            if hydro_id not in measurements_by_id.keys():
                measurements_by_id[hydro_id] = []
            # Kick out records with duplicate dates -- first in wins
            if len([x for x in measurements_by_id[hydro_id]
                    if x.Data.DiversionDate == each_measurement.Data.DiversionDate]) == 0:
                measurements_by_id[hydro_id].append(each_measurement)
                measurements_by_id[hydro_id].sort(key=lambda x: x.Data.DiversionDate)
        return measurements_by_id.values()
