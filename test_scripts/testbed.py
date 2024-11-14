#!/usr/bin/env python
import asyncio
import json
import sys
import os
import timeit
from sqlalchemy.sql.base import _NoArg

# from gulp.api.collab import context as collab_context
import muty.json
from gulp.config import GulpConfig
from typing import Optional, Type, TypeVar
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.stats import GulpIngestionStats
from gulp.api.collab.structs import GulpCollabType
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api import opensearch_api
from gulp.api.mapping.models import GulpMapping
from gulp.structs import GulpPluginParameters
from gulp.utils import GulpLogger
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy_mixins.serialize import SerializeMixin
from sqlalchemy.orm import MappedAsDataclass, DeclarativeBase, Mapped, mapped_column
from sqlalchemy import BIGINT, ForeignKey, String
from sqlalchemy.ext.asyncio import AsyncAttrs
from gulp.api.collab.structs import GulpCollabObject, GulpCollabType, GulpCollabBase
from gulp.api.collab.note import GulpNote
from gulp.api.collab.user import GulpUser
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab.user_data import GulpUserData
from dotwiz import DotWiz
from opensearchpy import AsyncOpenSearch
from sqlalchemy.ext.asyncio import AsyncEngine
from gulp.plugin import GulpPluginBase
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch_api import GulpOpenSearch
import yaml

_opt_samples_dir= os.environ.get('GULP_SAMPLES_DIR', '~/repos/gulp/samples')
_opt_samples_dir = os.path.expanduser(_opt_samples_dir)
_opt_reset = os.environ.get('GULP_RESET', False)
_opt_index = os.environ.get('GULP_INDEX', 'testidx')
_opt_gulp_integration_test = os.environ.get('GULP_INTEGRATION_TEST', False)
_operation='test_operation'
_context='test_context'
_test_req_id='test_req_id'
_test_ws_id='test_ws_id'
_guest_user='guest'
_admin_user='admin'


print('opt_samples_dir:', _opt_samples_dir)
print('opt_reset:', _opt_reset)
print('opt_index:', _opt_index)
print('opt_gulp_integration_test:', _opt_gulp_integration_test)

sigma_main_yaml = """
title: Filter Out Administrator accounts
description: Filters out administrator accounts that start with adm_
logsource:
  category: process_creation
  product: windows
filter:
  rules:
    - proc_creation_win_sc_create_service
    - proc_creation_win_custom
  selection:
    User|contains: "adm_"
  condition: not selection
"""
#sigma_main_yaml = yaml.dump(yaml.safe_load(sigma_main_yaml))
sigma_filter_1 = """ 
title: New Service Creation Using Sc.EXE
name: proc_creation_win_sc_create_service
id: 85ff530b-261d-48c6-a441-facaa2e81e48
related:
    - id: c02e96b7-c63a-4c47-bd83-4a9f74afcfb2 # Using PowerShell
      type: similar
status: test
description: Detects the creation of a new service using the "sc.exe" utility.
references:
    - https://github.com/redcanaryco/atomic-red-team/blob/f339e7da7d05f6057fdfcdd3742bfcf365fee2a9/atomics/T1543.003/T1543.003.md
author: Timur Zinniatullin, Daniil Yugoslavskiy, oscd.community
date: 2023-02-20
tags:
    - attack.persistence
    - attack.privilege-escalation
    - attack.t1543.003
logsource:
    category: process_creation
    product: windows
detection:
    selection:
        Image|endswith: '\\sc.exe'
        CommandLine|contains|all:
            - 'create'
            - 'binPath'
    condition: selection
falsepositives:
    - Legitimate administrator or user creates a service for legitimate reasons.
    - Software installation
level: low
filter:
  rules:
    - proc_creation_win_custom
  selection:
    User|startswith: "aaavalerino_"
  condition: not selection
"""
#sigma_filter_1 = yaml.dump(yaml.safe_load(sigma_filter_1))
sigma_filter_2 = """
title: New Service Creation Using Sc.EXE
name: proc_creation_win_custom
id: 85ff530b-261d-48c6-a441-facaa2e81e48
related:
    - id: c02e96b7-c63a-4c47-bd83-4a9f74afcfb2 # Using PowerShell
      type: similar
status: test
description: Detects the creation of a new service using the "sc.exe" utility.
references:
    - https://github.com/redcanaryco/atomic-red-team/blob/f339e7da7d05f6057fdfcdd3742bfcf365fee2a9/atomics/T1543.003/T1543.003.md
author: Timur Zinniatullin, Daniil Yugoslavskiy, oscd.community
date: 2023-02-20
tags:
    - attack.persistence
    - attack.privilege-escalation
    - attack.t1543.003
logsource:
    category: process_creation
    product: windows
detection:
    selection:
        Image|endswith: '\\bbbbvalerino.exe'
        CommandLine|contains|all:
            - 'create'
            - 'binPath'
    condition: selection
falsepositives:
    - Legitimate administrator or user creates a service for legitimate reasons.
    - Software installation
level: low
"""
#sigma_filter_2 = yaml.dump(yaml.safe_load(sigma_filter_2))

async def testbed():
    class MyModel(BaseModel):
        model_config = ConfigDict(extra="allow")
        field1: str = Field(..., description="required field", min_length=1)
        field2: str = Field("default", description="test field1")

        @staticmethod
        def test():
            m = MyModel(field1="123456")
            print('not set model_extra:', m.model_extra)
            mm = m.model_dump(exclude_none=True)
            mm['extra_shit'] = 'aaaa'
            m = MyModel.model_validate(mm)
            mm=m.model_dump(exclude_none=True)
            print('dumped model after setting model_extra', m)

    class TestPydanticClass(BaseModel):
        model_config = ConfigDict(extra="allow")
        field_required: dict = Field(..., description="required field", min_length=1)
        field2: str = Field("default", description="test field1")
        field1: int = Field("default", description="test field2")
        
        def __init__(self, **data):
            super().__init__(**data)
            self.field2 = "changed"
            
    class TestPydanticDerivedClass(TestPydanticClass):
        field3: str = Field(None, description="test field3")

    class TestOrmBase(MappedAsDataclass, AsyncAttrs, DeclarativeBase, SerializeMixin):
        """
        base for everything on the collab database
        """
        def __init_subclass__(cls, type: str, **kwargs) -> None:                
            cls.__tablename__ = type
            cls.__mapper_args__ = {
                "polymorphic_identity": type,
                "polymorphic_on": "type",
            }
            print(cls.__name__, cls.__tablename__, cls.__mapper_args__)
            super().__init_subclass__(cls, **kwargs)
        
        T = TypeVar("T", bound="TestOrm")

        id: Mapped[str] = mapped_column(
            String, primary_key=True, unique=True, doc="The id of the object."
        )
        type: Mapped[int] = mapped_column(String, doc="The type of the object.")
        time_created: Mapped[Optional[int]] = mapped_column(
            BIGINT,
            default=0,
            doc="The time the object was created, in milliseconds from unix epoch.",
        )
        __mapper_args__ = {
            "polymorphic_identity": "testorm_base",
            "polymorphic_on": "type",
        }
    class TestOrm(TestOrmBase, type='testorm'):        
        #__tablename__ = "testorm"
        id: Mapped[int] = mapped_column(ForeignKey("testorm_base.id"), primary_key=True)
        time_updated: Mapped[Optional[int]] = mapped_column(
            BIGINT,
            default=0,
            doc="The time the object was last updated, in milliseconds from unix epoch.",
        )
        __mapper_args__ = {"polymorphic_identity": "testorm"}

        @classmethod
        def print_name(cls):
            print(cls)
            print(TestOrm)
            print(cls.__name__)

    d = {"field1": 1, "field2": "aaa", "field_required": {"a": 1}}
    d.pop("field2")
    d.pop("eeee",None)
    print(d)
    return

    flt=GulpIngestionFilter(storage_ignore_filter=True)
    flt.time_range = {"start": 0, "end": 1}
    #for i in range(10):
    test_fun(flt)
    print('original', flt)
    return

    p = TestPydanticClass(field1=1, another_field="aaa", field_required={"a": 1})
    print(p)
    return
    d = p.model_dump()
    print(d)
    d={}
    print('validating...')
    pp = TestPydanticClass.model_validate(d)
    print(pp)
    return
    pp = TestPydanticClass(**d)
    print(pp)
    dd = pp.model_dump()
    print(dd)
    return

    TestOrm.print_name()
    return
    print("field1" in t.columns)
    print("time_created" in t.columns)
    tt = TestPydanticDerivedClass
    print("field1" in tt.model_fields)
    print("field4" in tt.model_fields)

async def test_init():

    #await testbed()
    #return
    GulpLogger.get_logger().debug("---> init")
    GulpConfig.get_instance()
    os = GulpOpenSearch.get_instance()
    collab = GulpCollab.get_instance()
    await collab.get_instance().init()
    if _opt_reset:
        GulpLogger.get_logger().debug("resetting...")
        await os.datastream_create(_opt_index)
    await collab.init(force_recreate=_opt_reset)
    
async def test_login_logout():
    GulpLogger.get_logger().debug("---> test_login_logout")
    session: GulpUserSession = await GulpUser.login(_guest_user, "guest")
    await GulpUser.logout(session.id)
    return

async def test_ingest_windows():
    GulpLogger.get_logger().debug("---> test_ingest_windows")
    
    # load plugin
    start_time = timeit.default_timer()
    file = os.path.join(_opt_samples_dir,'win_evtx/security_big_sample.evtx')
    plugin = await GulpPluginBase.load("win_evtx")
    
    # create stats upfront
    # 62031 ingested, 62031 processed, 0 failed
    stats: GulpIngestionStats = await GulpIngestionStats.create_or_get(
        _test_req_id, operation=_operation, context=_context, source_total=1)
    
    await plugin.ingest_file(_test_req_id, _test_ws_id, _guest_user, _opt_index, _operation, _context, file)
    end_time = timeit.default_timer()
    execution_time = end_time - start_time
    GulpLogger.get_logger().debug(
        "execution time for ingesting file %s: %f sec." % (file, execution_time)
    )

async def test_ingest_csv():
    GulpLogger.get_logger().debug("---> test_ingest_csv")
    
    # load plugin
    start_time = timeit.default_timer()
    file = os.path.join(_opt_samples_dir,'mftecmd/sample_j.csv')
    plugin = await GulpPluginBase.load("csv")
    
    # create stats upfront
    # 75 ingested, 75 processed, 0 failed
    stats: GulpIngestionStats = await GulpIngestionStats.create_or_get(_test_req_id, operation=_operation, context=_context, source_total=1)
    
    generic_mapping = GulpMapping(timestamp_field="UpdateTimestamp")
    params: GulpPluginParameters = GulpPluginParameters(mappings={"generic": generic_mapping}, model_extra={"delimiter": ","})  
    await plugin.ingest_file(_test_req_id, _test_ws_id, _guest_user, _opt_index, _operation, _context, file, plugin_params=params)
    end_time = timeit.default_timer()
    execution_time = end_time - start_time
    GulpLogger.get_logger().debug(
        "execution time for ingesting file %s: %f sec." % (file, execution_time)
    )

async def test_ingest_csv_with_mappings():
    GulpLogger.get_logger().debug("---> test_ingest_csv")
    
    # load plugin
    start_time = timeit.default_timer()
    file = os.path.join(_opt_samples_dir,'mftecmd/sample_record.csv')
    plugin = await GulpPluginBase.load("csv")
    
    # create stats upfront
    # 10 processed, 44 ingested, 0 failed
    stats: GulpIngestionStats = await GulpIngestionStats.create_or_get(_test_req_id, operation=_operation, context=_context, source_total=1)

    params: GulpPluginParameters = GulpPluginParameters(mapping_file="mftecmd_csv.json", mapping_id="record")
    await plugin.ingest_file(_test_req_id, _test_ws_id, _guest_user, _opt_index, _operation, _context, file, plugin_params=params)
    end_time = timeit.default_timer()
    execution_time = end_time - start_time
    GulpLogger.get_logger().debug(
        "execution time for ingesting file %s: %f sec." % (file, execution_time)
    )

async def test_ingest_csv_stacked():
    GulpLogger.get_logger().debug("---> test_ingest_csv_stacked")

    # load plugin
    start_time = timeit.default_timer()
    file = os.path.join(_opt_samples_dir,'mftecmd/sample_j.csv')
    plugin = await GulpPluginBase.load("stacked_example")
    
    # create stats upfront
    # 75 ingested, 75 processed, 0 failed, every document duration set to 9999
    stats: GulpIngestionStats = await GulpIngestionStats.create_or_get(_test_req_id, operation=_operation, context=_context, source_total=1)

    generic_mapping = GulpMapping(timestamp_field="UpdateTimestamp", agent_type="mftecmd", event_code="j")
    params: GulpPluginParameters = GulpPluginParameters(mappings={"generic": generic_mapping}, model_extra={"delimiter": ","})  
    await plugin.ingest_file(_test_req_id, _test_ws_id, _guest_user, _opt_index, _operation, _context, file, plugin_params=params)
    end_time = timeit.default_timer()
    execution_time = end_time - start_time
    GulpLogger.get_logger().debug(
        "execution time for ingesting file %s: %f sec." % (file, execution_time)
    )

async def test_bulk_insert():
    docs=[
        {
            "_id": "1",
            "@timestamp": "2016-11-17T16:54:58.794249+00:00",
            "gulp.timestamp": 1479401698794248960,
            "gulp.operation": "test_operation",
            "gulp.context": "test_context",
            "agent.type": "mftecmd",
            "event.original": "...",
            "event.sequence": 0,
            "event.code": "record_modified_0x10",
            "gulp.event.code": 14872615,
            "event.duration": 1,
            "log.file.path": "/home/valerino/repos/gulp/samples/mftecmd/sample_record.csv",
            "gulp.unmapped.SequenceNumber": "1",
            "gulp.unmapped.InUse": "True",
            "gulp.unmapped.ParentEntryNumber": "5",
            "gulp.unmapped.ParentSequenceNumber": "5",
            "file.directory": ".",
            "file.name": "$MFT",
            "file.size": 683933696,
        },
        {
            "_id": "2",
            "@timestamp": "2016-11-17T17:54:58.794249+00:00",
            "gulp.timestamp": 1479401798794248960,
            "gulp.operation": "test_operation",
            "gulp.context": "test_context",
            "agent.type": "mftecmd",
            "event.original": "...",
            "event.sequence": 0,
            "event.code": "record_modified_0x10",
            "gulp.event.code": 14872615,
            "event.duration": 1,
            "log.file.path": "/home/valerino/repos/gulp/samples/mftecmd/sample_record.csv",
            "gulp.unmapped.SequenceNumber": "1",
            "gulp.unmapped.InUse": "True",
            "gulp.unmapped.ParentEntryNumber": "5",
            "gulp.unmapped.ParentSequenceNumber": "5",
            "file.directory": ".",
            "file.name": "$MFT",
            "file.size": 683933696,
        }
    ]
    await GulpNote.bulk_create_from_documents(docs, 
                                              ws_id=None,req_id='123', user_id='admin', title="test match", tags=['tag1','tag2'])

async def test_sigma_convert():
    GulpLogger.get_logger().debug("---> test_sigma_convert")
    
    # load plugin
    plugin = await GulpPluginBase.load("win_evtx")
    
    rules = plugin.sigma_convert(sigma_main_yaml, referenced_sigmas=[sigma_filter_1, sigma_filter_2])
    print(rules)

async def main():
    try:       
        await test_init()

        #await test_ingest_windows()
        #await test_ingest_csv()
        await test_ingest_csv_with_mappings()
        #await test_ingest_csv_stacked()
        #await test_bulk_insert()
        #await GulpOperation.add_context(_operation, _context)
        #await test_sigma_convert()

    finally:
        await GulpOpenSearch.get_instance().shutdown()

if __name__ == "__main__":
    asyncio.run(main())
