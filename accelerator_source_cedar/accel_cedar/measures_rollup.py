import logging
import pandas as pd

from accelerator_source_cedar.accel_cedar.cedar_intermediate_model import MeasuresArrays

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(filename)s:%(funcName)s:%(lineno)d: %(message)s"

)
logger = logging.getLogger(__name__)


class PcorMeasuresRollupStructure:

    def __init__(self, parent, subcategory_major, subcategory_minor, measure):
        self.parent = parent
        self.subcategory_major = subcategory_major
        self.subcategory_minor = subcategory_minor
        self.measure = measure


class MeasuresRollup:

    def __init__(self, measures_file):
        """

        Parameters
        ----------
        pcor_ingest_configuration PcorIngestConfiguration that contains configuration from properties file
        """
        self.measures_file = measures_file
        self.measures = self.build_measures_structure()

    def measures_rollup_as_dataframe(self):
        """
        Get the data frame (pandas) from the measures spreadsheet

        Returns
        -------
        pandas.DataFrame representing the contents of the measures rollup

        """
        logger.info("measures_rollup_as_dataframe")
        df = pd.read_excel(self.measures_file, sheet_name='ForExport', engine='openpyxl')
        return df

    def build_measures_structure(self):

        """
        create a structure by measure that contains the rollup information

        Returns Dictionary with key of measure and value of PcorMeasuresRollup
        -------

        """

        logger.info("init_measures_structure()")
        df = self.measures_rollup_as_dataframe()
        measures_dict = {}

        ss_rows = df.shape[0]

        for i in range(ss_rows):
            if isinstance(df.iat[i, 3], str):
                measure = PcorMeasuresRollupStructure(MeasuresRollup.filter_blank_measure(df.iat[i, 0]),
                                                      MeasuresRollup.filter_blank_measure(df.iat[i, 1]),
                                                      MeasuresRollup.filter_blank_measure(df.iat[i, 2]),
                                                      MeasuresRollup.filter_blank_measure(df.iat[i, 3]))
                #logger.info("measure:%s" % measure)
                measures_dict[MeasuresRollup.filter_blank_measure(df.iat[i, 3])] = measure

        return measures_dict

    @staticmethod
    def filter_blank_measure(measure):
        if isinstance(measure, str):
            return measure
        else:
            return "Other"

    def lookup_measure(self, measure):
        """
        For a given measure, return the rollup
        Parameters
        ----------
        measure - str with the measure

        Returns PcorMeasuresRollupStructure associated with the measure
        -------
        """

        rollup = self.measures.get(measure)

        if rollup:
            return rollup
        else:
            return PcorMeasuresRollupStructure("Other", "Other", "Other", measure)

    def process_measures(self, measures):
        """
        for an array of measures, return three arrays which are the measures rollup for each
        of the provided measures with duplicates filtered

        Parameters
        ----------
        measures - str[] with the measures

        Returns MeasuresArrays with the complete rollup of each measure
        -------
        """

        measures_arrays = MeasuresArrays()

        for measure in measures:

            if measure not in measures_arrays.measures:
                measures_arrays.measures.append(measure)
                measure_rollup = self.lookup_measure(measure)

                if measure_rollup.parent not in measures_arrays.measures_parents:
                    measures_arrays.measures_parents.append(measure_rollup.parent)

                if measure_rollup.subcategory_major not in measures_arrays.measures_subcategories_major:
                    measures_arrays.measures_subcategories_major.append(measure_rollup.subcategory_major)

                if measure_rollup.subcategory_minor not in measures_arrays.measures_subcategories_minor:
                    measures_arrays.measures_subcategories_minor.append(measure_rollup.subcategory_minor)

        return measures_arrays
