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
        ),
    },
    2019: {
        Month.FEBRUARY: ArchiveCopier(
            folder_root=Path(r'I:\Train\Report Data\2019 Data\2019_02_February'),
            file_name='2019_02_ProviderALLDATA',
            file_sheet='freedmanc@47100'
        ),
        Month.AUGUST: ArchiveComposer(
            folder_root=Path(r'I:\Train\Report Data\2019 Data\2019_08_August'),
            general_file='2019_08_freedmanc@47100-PDRGENERAL',
            general_sheet='2019_08_freedmanc@47100-PDRGENE',
            care_center_file='2019_08_NW_Provider_Care Center_2',
            care_center_sheet='freedmanc@47100-CARE_CENTER',
            address_file='2019_08_freedmanc@47100-ADDRESS',
            address_sheet=None
        ),
    },
    2018: {
        Month.JANUARY: ArchiveComposer(
            folder_root=Path(r'I:\Train\Report Data\2018 Data\2018_01_January'),
            general_file= '2018_01_NW_PDR_GENERAL',
            general_sheet=None,
            care_center_file= '2018_01_NW_PDR_CARE_CENTER',
            care_center_sheet=None,
            address_file= '2018_01_NW_PDR_ADDRESS',
            address_sheet=None
        ),
        Month.FEBRUARY: ArchiveComposer(
            folder_root=Path(r'I:\Train\Report Data\2018 Data\2018_02_February'),
            general_file= '2018_01_NW_PDRGENERAL',
            general_sheet=None,
            care_center_file= '2018_02_NW_CARE_CENTER',
            care_center_sheet=None,
            address_file= '2018_02_NW_ADDRESS',
            address_sheet=None
        ),
        Month.MARCH: ArchiveCopier(
            folder_root=Path(r'I:\Train\Report Data\2018 Data\2018_03_March'),
            file_name='2018_03_NW_PDR_ALL',
            file_sheet=None
        ),
        Month.APRIL: ArchiveComposer(
            folder_root=Path(r'I:\Train\Report Data\2018 Data\2018_04_April'),
            general_file= '2018_04_nw_pdr_general',
            general_sheet=None,
            care_center_file= '2018_04_nw_pdr_care_center',
            care_center_sheet=None,
            address_file= '2018_04_nw_pdr_address',
            address_sheet=None
        ),
        Month.MAY: ArchiveComposer(
            folder_root=Path(r'I:\Train\Report Data\2018 Data\2018_05_May'),
            general_file= '2018_05_nw_pdr_general',
            general_sheet=None,
            care_center_file= '2018_05_nw_pdr_carecenter',
            care_center_sheet=None,
            address_file= '2018_05_nw_pdr_address',
            address_sheet=None
        ),
        
        Month.JUNE: ArchiveComposer(
            folder_root=Path(r'I:\Train\Report Data\2018 Data\2018_06_June'),
            general_file= '2018_06_nw_pdr_general',
            general_sheet=None,
            care_center_file= '2018_06_nw_pdr_carecenter',
            care_center_sheet=None,
            address_file= '2018_06_nw_pdr_address',
            address_sheet=None
        ),
        Month.JULY: ArchiveCopier(
            folder_root=Path(r'I:\Train\Report Data\2018 Data\2018_07_July'),
            file_name='2018_07_nw_pdr_alldata',
            file_sheet=None,
        ),
        Month.AUGUST: ArchiveComposer(
            folder_root=Path(r'I:\Train\Report Data\2018 Data\2018_08_August'),
            general_file='2018_08_PDR_PDRGENERAL',
            general_sheet=None,
            care_center_file='2018_08_PDR_CARE_CENTER',
            care_center_sheet=None,
            address_file='2018_08_PDR_ADDRESS',
            address_sheet=None
        ),
        Month.SEPTEMBER: ArchiveComposer(
            folder_root=Path(r'I:\Train\Report Data\2018 Data\2018_09_September'),
            general_file='2018_09_nw_pdr_general',
            general_sheet=None,
            care_center_file='2018_09_nw_pdr_carecenter',
            care_center_sheet=None,
            address_file='2018_09_nw_pdr_address',
            address_sheet=None
        ),
        Month.OCTOBER: ArchiveComposer(
            folder_root=Path(r'I:\Train\Report Data\2018 Data\2018_10_October'),
            general_file='2018_10_nw_pdr_general',
            general_sheet=None,
            care_center_file='2018_10_nw_pdr_carecenter',
            care_center_sheet=None,
            address_file='2018_10_nw_pdr_address',
            address_sheet=None
        ),
        Month.NOVEMBER: ArchiveCopier(
            folder_root=Path(r'I:\Train\Report Data\2018 Data\2018_11_November'),
            file_name='2018_11_NW_AllProviderData',
            file_sheet=None,
        ),
        Month.DECEMBER: ArchiveCopier(
            folder_root=Path(r'I:\Train\Report Data\2018 Data\2018_12_December'),
            file_name='2018_12_NW_AllProviderData',
            file_sheet=None,
        ),
    },
    2017: {
        Month.MARCH: ArchiveComposer(
            folder_root=Path(r'I:\Train\Report Data\2017 Data\2017_03_March'),
            general_file='CCOA_PDRGeneral',
            general_sheet=None,
            care_center_file='CCOA_PDRCareCenter',
            care_center_sheet='CCOA_PDRCareCenter',
            address_file='CCOA_PDRAddress',
            address_sheet=None
        ),
        Month.AUGUST: ArchiveComposer(
            folder_root=Path(r'I:\Train\Report Data\2017 Data\2017_08_August'),
            general_file='freedmanc@47100-PDRGENERAL(1)',
            general_sheet=None,
            care_center_file='freedmanc@47100-CARE_CENTER',
            care_center_sheet=None,
            address_file='freedmanc@47100-ADDRESS',
            address_sheet=None
        ),
        Month.SEPTEMBER: ArchiveComposer(
            folder_root=Path(r'I:\Train\Report Data\2017 Data\2017_09_September'),
            general_file='2017_09_pdr_General',
            general_sheet=None,
            care_center_file='2017_09_pdr_CARE_CENTER',
            care_center_sheet='freedmanc@47100-CARE_CENTER',
            address_file='2017_09_pdr_ADDRESS',
            address_sheet=None
        ),
        Month.DECEMBER: ArchiveComposer(
            folder_root=Path(r'I:\Train\Report Data\2017 Data\2017_12_December'),
            general_file='2017_12_NW_PDR_GENERAL',
            general_sheet=None,
            care_center_file='2017_12_NW_PDR_CARECENTER',
            care_center_sheet=None,
            address_file='2017_12_NW_PDR_ADDRESS',
            address_sheet=None
        ),
    }
}
