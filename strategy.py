from abc import ABC, abstractmethod
from sqlalchemy import create_engine
import pandas as pd

# template of program using strategy pattern

def _check_methods(c, *methods):
    mro = c.__mro__
    for method in methods:
        for B in mro:
            if method in B.__dict__:
                if B.__dict__[method] is None:
                    return NotImplemented
                break
        else:
            return NotImplemented
    return True


class AbstractCommandSet(ABC):

    @abstractmethod
    def execute_command(self, *args, **kwargs):
        pass

    @abstractmethod
    def define_connection(self, *args, **kwargs):
        pass

    @abstractmethod
    def execute_process(self, *args, **kwargs):
        pass

    @classmethod
    def __subclasshook__(cls, C):
        if cls is AbstractCommandSet:
            return _check_methods(C, "__hash__")
        return NotImplemented


class AbstractToolset(ABC):
    def __init__(self):
        self.settings = None
        self.tool = None
        self.params = None

    def set_params(self, params):
        self.params = params
        pass

    def set_settings(self, settings):
        self.settings = settings

    @abstractmethod
    def select_tool(self, *args, **kwargs):
        pass


class FirstLogicHandlerForOSone(AbstractCommandSet):
    def __init__(self,params:dict, settings:dict):
        self.alchemy_engine = None
        self.params = params
        self.settings = settings

    def execute_command(self, command):
        return self.alchemy_engine.execute(command)

    def define_connection(self, connstring:str):
        self.alchemy_engine = create_engine(connstring)


    def custom_piece_of_logic_etl(self):
        # logic example
        sql = """select distinct id, prod_name, some_info 
                 from some_table 
                 where some_column NOTNULL"""
        result = self.execute_command(sql).fetchall()
        df1 = pd.DataFrame(result)

        # some transformations

        df1.to_sql("new_tab", self.alchemy_engine, schema="some_fancy_schema", if_exists="append")

    def execute_process(self):
        connstring = self.settings.get('some_connstring')
        self.define_connection(connstring)
        self.custom_piece_of_logic_etl()


class SecondLogicHandlerForOSone(AbstractCommandSet):
    def execute_command(self, command):
        pass

    def define_connection(self, *args, **kwargs):
        pass

    def custom_piece_of_logic(self):
        pass

    def execute_process(self, *args, **kwargs):
        pass


class FirstLogicHandlerForOStwo(AbstractCommandSet):
    def execute_command(self, command):
        pass

    def define_connection(self, *args, **kwargs):
        pass

    def custom_piece_of_logic(self):
        pass

    def execute_process(self, *args, **kwargs):
        pass


class SecondLogicHandlerForOStwo(AbstractCommandSet):
    def execute_command(self, command):
        pass

    def define_connection(self, *args, **kwargs):
        pass

    def custom_piece_of_logic(self):
        pass

    def execute_process(self, *args, **kwargs):
        pass




class OsFirstToolset(AbstractToolset):
    def select_tool(self, msg):
        if msg == "command1":
            self.tool = FirstLogicHandlerForOSone()
        if msg == "command2":
            self.tool = SecondLogicHandlerForOSone()

class OsSecondToolset(AbstractToolset):

    def select_tool(self, msg):
        if msg == "command1":
            self.tool = FirstLogicHandlerForOStwo()
        if msg == "command2":
            self.tool = SecondLogicHandlerForOStwo()



class Dispatcher:

    def __init__(self, settings:dict, cli_params:dict):
        self.cli_params = cli_params
        self.settings = settings
        self.toolset = None

    def set_toolset(self, platform:str):
        if platform:
            if platform == "os1":
                self.toolset = OsFirstToolset()
            elif platform == "os2":
                self.toolset = OsSecondToolset()

    def set_toolset_state(self, platform:str, command:str):
        self.set_toolset(platform)
        self.toolset.select_tool(command)
        self.toolset.set_params(self.cli_params)
        self.toolset.set_settings(self.settings)

    def execute_logic(self):
        platform = self.cli_params.get("platform")
        command = self.cli_params.get("command")
        self.set_toolset_state(platform, command)
        self.toolset.tool.execute_proces(params=self.cli_params, settings=self.settings)


