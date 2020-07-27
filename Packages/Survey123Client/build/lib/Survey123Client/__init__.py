from arcgis.gis import GIS


class Survey123Client:
    """
    Client for downloading data from Survey123 feature services
    """
    def __init__(self, url: str, username: str, password: str):
        self.__gis = GIS(
            url=url,
            username=username,
            password=password
        )

    def retrieve_survey_results(self, survey_id: str, field_dict: dict, where_clause='1=1') -> dict:
        """
        Retrieve results from one Survey123 feature service.
        :param survey_id: ID of the feature service (typically a GUID, I think)
        :param field_dict: Dictionary that maps names of feature service fields with what the rest of the application
                    would rather call those fields
        :param where_clause: where clause to limit returns from the feature service. Omitting this param is equivalent
                    to '1=1'
        :return: Dictionary of dictionaries. Outer dictionary is keyed by ObjectID from the feature service and inner
                    dictionaries represent individual features
        """
        survey_results_layer = self.__gis.content.get(survey_id).layers[0]
        object_id_field = survey_results_layer.properties["objectIdField"]
        fields_to_request = list(field_dict.values())
        fields_to_request.append(object_id_field)
        fields = ",".join(fields_to_request)
        if not where_clause:
            where_clause = '1=1'
        survey_features = survey_results_layer.query(where=where_clause, out_fields=fields).features

        return_dict = {}

        for r in survey_features:
            row_dict = {}
            for (database_field_name, service_field_name) in field_dict.items():
                row_dict[database_field_name] = r.get_value(service_field_name)
            return_dict[r.get_value(object_id_field)] = row_dict

        return return_dict
