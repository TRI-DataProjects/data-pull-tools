from enum import Enum
from pathlib import Path

from ArchiveCollector import *


class Month(Enum):
    JANUARY = '01'
    FEBRUARY = '02'
    MARCH = '03'
    APRIL = '04'
    MAY = '05'
    JUNE = '06'
    JULY = '07'
    AUGUST = '08'
    SEPTEMBER = '09'
    OCTOBER = '10'
    NOVEMBER = '11'
    DECEMBER = '12'


ARCHIVE_COLLECTORS = {
    2020: {
        Month.JANUARY: ArchiveCopier(
            folder_root=Path(r'I:\Train\Report Data\2020 Data\2020_01_January'),
            file_name='freedmanc@47100_ALL PROVIDER',
            file_sheet='freedmanc@47100_ALL PROVIDER'
        ),
        Month.AUGUST: ArchiveCopier(
            folder_root=Path(r'I:\Train\Report Data\2020 Data\2020_08_August'),
            file_name='2020_08_Provider_ALL',
            file_sheet='freedmanc@47100'
        )
    },
    2019: {
        Month.FEBRUARY: ArchiveCopier(
            folder_root=Path(r'I:\Train\Report Data\2019 Data\2019_02_February'),
            file_name='2019_02_ProviderALLDATA',
            file_sheet = 'freedmanc@47100'
        ),
        Month.AUGUST: ArchiveComposer(
            folder_root=Path(r'I:\Train\Report Data\2019 Data\2019_08_August'),
            general_file='2019_08_freedmanc@47100-PDRGENERAL',
            general_sheet='2019_08_freedmanc@47100-PDRGENE',
            care_center_file='2019_08_NW_Provider_Care Center_2',
            care_center_sheet='freedmanc@47100-CARE_CENTER',
            address_file='2019_08_freedmanc@47100-ADDRESS'
        )
    },
    2018: {
        Month.JANUARY: ArchiveComposer(
            folder_root=Path(r'I:\Train\Report Data\2018 Data\2018_01_January'),
            general_file= '2018_01_NW_PDR_GENERAL',
            care_center_file= '2018_01_NW_PDR_CARE_CENTER',
            address_file= '2018_01_NW_PDR_ADDRESS'
        ),
        Month.AUGUST: ArchiveComposer(
            folder_root=Path(r'I:\Train\Report Data\2018 Data\2018_08_August'),
            general_file='2018_08_PDR_PDRGENERAL',
            care_center_file='2018_08_PDR_CARE_CENTER',
            address_file='2018_08_PDR_ADDRESS'
        )
    },
    2017: {
        Month.MARCH: ArchiveComposer(
            folder_root=Path(r'I:\Train\Report Data\2017 Data\2017_03_March'),
            general_file='CCOA_PDRGeneral',
            care_center_file='CCOA_PDRCareCenter',
            care_center_sheet='CCOA_PDRCareCenter',
            address_file='CCOA_PDRAddress'
        ),
        Month.AUGUST: ArchiveComposer(
            folder_root=Path(r'I:\Train\Report Data\2017 Data\2017_08_August'),
            general_file='freedmanc@47100-PDRGENERAL(1)',
            care_center_file='freedmanc@47100-CARE_CENTER',
            address_file='freedmanc@47100-ADDRESS'
        ),
        Month.SEPTEMBER: ArchiveComposer(
            folder_root=Path(r'I:\Train\Report Data\2017 Data\2017_09_September'),
            general_file='2017_09_pdr_General',
            care_center_file='2017_09_pdr_CARE_CENTER',
            care_center_sheet='freedmanc@47100-CARE_CENTER',
            address_file='2017_09_pdr_ADDRESS'
        ),
        Month.DECEMBER: ArchiveComposer(
            folder_root=Path(r'I:\Train\Report Data\2017 Data\2017_12_December'),
            general_file='2017_12_NW_PDR_GENERAL',
            care_center_file='2017_12_NW_PDR_CARECENTER',
            address_file='2017_12_NW_PDR_ADDRESS'
        )
    }
}
