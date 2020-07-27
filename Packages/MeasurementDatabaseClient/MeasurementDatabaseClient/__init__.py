from dataclasses import dataclass
import datetime


class DeviceType:
    """Enumeration-ish thing reflecting measurement device types"""
    OpenChannel = "Open_Channel"
    ClosedConduit = "Closed_Conduit"

    @classmethod
    def parse(cls, string: str):
        if "open" in string.lower():
            return cls.OpenChannel
        return cls.ClosedConduit


@dataclass
class WdHydrologyPD(object):
    """Object representing one row in MeasurementDatabase.dbo.wdHydrologyPD"""
    ID: str
    HydrologyId: int
    WaterDistrictNumber: str
    DiversionTypeId: int
    DiversionName: str
    ReachDescription: str
    WaterDistPDID: str
    Comment: str
    Inactive: bool
    LocationId: int
    DiversionLocationId: int


@dataclass
class WdWaterMasterData(object):
    """Object representing one row in MeasurementDatabase.dbo.WdWaterMasterData"""
    #    ID: str
    WdHydrologyPdId: str
    HydrologyId: int
    DiversionDate: datetime.date
    MeasurementTypeId: int
    Discharge: float
    RegistrationId: str
    UserId: str


@dataclass
class WdWaterMasterDataMetadata(object):
    """Represents one row in MeasurementDatabase.dbo.WdWaterMasterData plus metadata necessary for processing"""
    Data: WdWaterMasterData
    DeviceType: DeviceType


@dataclass
class MeasurementDatabaseInvalidData(object):
    """Object representing one row with invalid data"""
    ID: str
    Message: str
