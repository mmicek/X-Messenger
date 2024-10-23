class ApplicationService:

    def __init__(self, context):
        self.context = context
        self.applications_settings_dict = (
            {}
        )  # Application identifier -> Application object
        self.applications_dict = {}

    def clear_application_settings(self):
        self.applications_settings_dict.clear()

    def set_application_settings(self, application_identifier, application):
        self.applications_settings_dict[application_identifier] = application

    def get_applications_settings(self):
        return self.applications_settings_dict

    def increase_user_count(self, application_identifier):
        if application_identifier not in self.applications_settings_dict:
            return False
        application_settings = self.applications_settings_dict[application_identifier]
        if not application_settings["is_chat_active"]:
            return False

        if application_identifier not in self.applications_dict:
            self.applications_dict[application_identifier] = 0

        max_concurrent_online_users = application_settings[
            "max_concurrent_online_users"
        ]
        if (
            self.applications_dict[application_identifier]
            >= max_concurrent_online_users
        ):
            return False
        self.applications_dict[application_identifier] += 1
        return True

    def reduce_user_count(self, application_identifier):
        if application_identifier in self.applications_dict:
            self.applications_dict[application_identifier] -= 1

    def get_applications_dict(self):
        return self.applications_dict
