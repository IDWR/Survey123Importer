import datetime

import pyodbc

from MeasurementDatabaseClient import WdHydrologyPD, WdWaterMasterData
from MeasurementDatabaseClient.exceptions import AlreadyGotOneException, InvalidDataException


class Repository:
    """
    Abstract Repository class copied from someplace on the internet that I now cannot find
    """

    def __init__(self, connection_string: str):
        self.conn = self.__get_connection__(connection_string)
        self._complete = False

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        # can test for type and handle different situations
        self.close()

    def complete(self):
        self._complete = True

    def close(self):
        if self.conn:
            try:
                if self._complete:
                    self.conn.commit()
                else:
                    self.conn.rollback()
            finally:
                self.conn.close()

    @staticmethod
    def __get_connection__(connection_string):
        return pyodbc.connect(connection_string)


class WdHydrologyPdRepository(Repository):
    """
    Repository for the WdHydrologyPd table
    """
    __select_all_fields = ('SELECT [ID]'
                           ',[HydrologyID]'
                           ',[WaterDistrictNumber]'
                           ',[DiversionTypeID]'
                           ',[DiversionName]'
                           ',[ReachDescription]'
                           ',[WaterDistPDID]'
                           ',[Comment]'
                           ',[Inactive]'
                           ',[LocationID]'
                           ',[DiversionLocationID]'
                           'FROM [MeasurementDatabase].[dbo].[vwwdHydrologyPD]')

    def get_by_hydro_id(self, hydro_id: int):
        """
        Gets one WdHydrologyPd record based on HydrologyID
        :param hydro_id: HydrologyID of the record to return
        :return: WdHydrologyPd record
        """
        c = self.conn.cursor()
        c.execute(self.__select_all_fields + ' where HydroID = ?', hydro_id)
        return self.__make_object_from_row__(c.fetchone())

    def get_by_location_id(self, location_id: int):
        """
        Gets one WdHydrologyPd record based on LocationID
        :param location_id: LocationID of the record to return
        :return: WdHydrologyPd record
        """
        c = self.conn.cursor()
        c.execute(self.__select_all_fields + ' where LocationID = ?', location_id)
        row = c.fetchone()
        return None if row is None else self.__make_object_from_row__(row)

    def get_by_water_district(self, water_district_number: str):
        """
        Gets a list of WdHydrologyPd records based on WaterDistrictNumber
        :param water_district_number: WaterDistrictNumber of the records to return
        :return: WdHydrologyPd record
        """
        c = self.conn.cursor()
        c.execute(self.__select_all_fields + ' where WaterDistrictNumber = ?', water_district_number)
        rows = c.fetchall()
        return [self.__make_object_from_row__(row) for row in rows]

    @staticmethod
    def __make_object_from_row__(row):
        """
        Creates a WdHydrologyPD object from a row retrieved from a pyodbc cursor
        :param row: Row from a pyodbc cursor
        :return: WdHydrologyPD object
        """
        pd = WdHydrologyPD(
            HydrologyId=row.HydrologyID,
            WaterDistPDID=row.WaterDistPDID,
            Comment=row.Comment,
            DiversionName=row.DiversionName,
            DiversionTypeId=row.DiversionTypeID,
            ID=row.ID,
            Inactive=row.Inactive,
            DiversionLocationId=row.PodSpatialDataID,
            ReachDescription=row.ReachDescription,
            LocationId=row.SpatialDataID,
            WaterDistrictNumber=row.WaterDistrictNumber
        )
        return pd


class WdWaterMasterDataRepository(Repository):
    """
    Repository for the WdWaterMasterData table
    """

    def get_by_hydro_id_and_diversion_date(self, hydro_id: int, diversion_date: datetime.date):
        """
        Gets a WdWaterMasterData record by HydrologyID and DiversionDate
        :param hydro_id: HydrologyID value for which to search
        :param diversion_date: DiversionDate value for which to search
        :return:
        """
        c = self.conn.cursor()
        c.execute('SELECT '
                  '     [WdHydrologyPdId], '
                  '     [HydrologyId], '
                  '     [DiversionDate], '
                  '     [MeasurementTypeId], '
                  '     [Discharge], '
                  '     [RegistrationId], '
                  '     [UserId] '
                  'FROM [vwwdWaterMasterData] '
                  'WHERE [HydrologyID]=? and [DiversionDate]=?',
                  (hydro_id, diversion_date))
        return self.__construct_data_from_row(c.fetchone())

    def get_last_measurement_at_hydro_id(self, hydro_id: int, limit_to_this_year=True):
        """
        Gets the last measurement at a particular diversion
        :param hydro_id:  HydrologyID of the diversion to search
        :param limit_to_this_year:  Boolean indicating whether to search only in the current year (default is True)
        :return:  WdWaterMasterData record representing the last measurement taken at the indicated HydrologyID
        """
        year_limit = 'AND YEAR([DiversionDate]) = YEAR(GETDATE())' if limit_to_this_year else ''
        c = self.conn.cursor()
        c.execute('SELECT TOP 1'
                  '     [WdHydrologyPdId], '
                  '     [HydrologyId], '
                  '     [DiversionDate], '
                  '     [MeasurementTypeId], '
                  '     [Discharge], '
                  '     [RegistrationId], '
                  '     [UserId] '
                  'FROM [vwwdWaterMasterData] '
                  'WHERE [HydrologyID]=? '
                  ' {0} '
                  'ORDER BY [DiversionDate] DESC'.format(year_limit),
                  hydro_id)
        return self.__construct_data_from_row(c.fetchone())

    def get_last_data_date_for_hydro_id(self, hydro_id: int):
        return self.get_last_measurement_at_hydro_id(hydro_id=hydro_id).DiversionDate

    def add_measurement(self, wd_water_master_data: WdWaterMasterData):
        """
        Inserts one measurement to the WdWaterMasterData table
        :param wd_water_master_data: Data to be inserted
        :return: Nothing
        """
        try:
            self.__validate_data__(wd_water_master_data)

            c = self.conn.cursor()

            values = (
                wd_water_master_data.DiversionDate,
                wd_water_master_data.MeasurementTypeId,
                wd_water_master_data.Discharge,
                None,  # GageHeight
                None,  # GageHeightShift
                None,  # Evaporation
                None,  # Precipitation
                wd_water_master_data.HydrologyId,
                wd_water_master_data.RegistrationId,
                wd_water_master_data.UserId,
                wd_water_master_data.WdHydrologyPdId
            )

            sql = "exec [dbo].[spInsertDiversionData] " \
                  "@DiversionDate = ?," \
                  "@MeasurementTypeID = ?," \
                  "@Discharge = ?," \
                  "@GageHeight = ?," \
                  "@GageHeightShift = ?," \
                  "@Evaporation = ?," \
                  "@Precipitation = ?," \
                  "@HydrologyID = ?," \
                  "@RegistrationID = ?," \
                  "@UserID = ?," \
                  "@HydrologyPDID = ?"

            c.execute(sql, values)
        except pyodbc.IntegrityError as e:
            if 'Unique wdWaterMasterData' in str(e):
                raise AlreadyGotOneException(wd_water_master_data.HydrologyId, wd_water_master_data.DiversionDate)
            raise

    @staticmethod
    def __construct_data_from_row(row):
        if not row:
            return None
        return WdWaterMasterData(
            WdHydrologyPdId=row.WdHydrologyPdId,
            HydrologyId=row.HydrologyId,
            DiversionDate=row.DiversionDate.date(),
            MeasurementTypeId=row.MeasurementTypeId,
            Discharge=float(row.Discharge),
            RegistrationId=row.RegistrationId,
            UserId=row.UserId
        )

    def __validate_data__(self, wd_water_master_data: WdWaterMasterData):
        """
        Validates data before entry
        """
        invalid_list = []
        if wd_water_master_data.HydrologyId is None or wd_water_master_data.HydrologyId < 1:
            invalid_list.append("HydrologyId")
        if wd_water_master_data.MeasurementTypeId is None or wd_water_master_data.MeasurementTypeId < 1:
            invalid_list.append("MeasurementTypeId")
        if wd_water_master_data.DiversionDate is None:
            invalid_list.append("DiversionDate")
        if wd_water_master_data.UserId is None or len(wd_water_master_data.UserId) == 0:
            invalid_list.append("UserId")

        if self.get_by_hydro_id_and_diversion_date(
                wd_water_master_data.HydrologyId,
                wd_water_master_data.DiversionDate) is not None:
            raise AlreadyGotOneException(wd_water_master_data.HydrologyId, wd_water_master_data.DiversionDate)

        if len(invalid_list) == 0:
            return True
        raise InvalidDataException(field_list=invalid_list)
