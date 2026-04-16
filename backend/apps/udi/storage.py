"""
Storage layer for the Universal Data Index (UDI) system.

Provides SQL database persistence for cached data with thread-safe operations.
"""

import json
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

from apps.core.logging_config import setup_logging

logger = setup_logging(__name__)


class UDIStorage:
    """SQL-based storage for UDI data with thread-safe operations."""
    
    def __init__(self, storage_dir: Optional[Path] = None):
        """Initialize the UDI storage with DatabaseManager."""
        from apps.database.manager import get_db_manager
        self.db = get_db_manager()
        logger.info("UDI storage initialized with SQL backend Manager.")
    
    def _channel_to_dict(self, c) -> dict:
        if not c: return None
        res = {col.name: getattr(c, col.name) for col in c.__table__.columns}
        res['streams'] = [s.id for s in c.streams]
        return res

    def _stream_to_dict(self, s) -> dict:
        if not s: return None
        res = {col.name: getattr(s, col.name) for col in s.__table__.columns}
        if s.updated_at: res['updated_at'] = s.updated_at.isoformat()
        if s.last_seen: res['last_seen'] = s.last_seen.isoformat()
        if hasattr(s, 'stats_updated_at') and s.stats_updated_at: 
            res['stats_updated_at'] = s.stats_updated_at.isoformat()
        return res

    def _group_to_dict(self, g) -> dict:
        if not g: return None
        return {col.name: getattr(g, col.name) for col in g.__table__.columns}

    def _logo_to_dict(self, l) -> dict:
        if not l: return None
        return {col.name: getattr(l, col.name) for col in l.__table__.columns}

    def _account_to_dict(self, a) -> dict:
        if not a: return None
        res = {col.name: getattr(a, col.name) for col in a.__table__.columns}
        if a.created_at: res['created_at'] = a.created_at.isoformat()
        if a.updated_at: res['updated_at'] = a.updated_at.isoformat()
        return res

    def _profile_to_dict(self, p) -> dict:
        if not p: return None
        return {col.name: getattr(p, col.name) for col in p.__table__.columns}

    # Channels
    def load_channels(self) -> List[Dict[str, Any]]:
        from apps.database.models import Channel
        from apps.database.connection import get_session
        session = get_session()
        try:
            channels = session.query(Channel).all()
            return [self._channel_to_dict(c) for c in channels]
        finally:
            session.close()
    
    def save_channels(self, channels: List[Dict[str, Any]]) -> bool:
        from apps.database.models import Channel
        from apps.database.connection import get_session
        session = get_session()
        try:
            for item in channels:
                chan_id = item.get('id')
                c = session.query(Channel).filter(Channel.id == chan_id).first()
                if not c:
                    c = Channel(id=chan_id)
                    session.add(c)
                for k,v in item.items():
                    if k != 'streams' and hasattr(c, k):
                         setattr(c, k, v)
                if 'streams' in item:
                    from apps.database.models import Stream
                    c.streams = session.query(Stream).filter(Stream.id.in_(item['streams'])).all()
            session.commit()
            self._update_metadata('channels_last_updated')
            return True
        except:
            session.rollback()
            return False
        finally:
            session.close()
    
    def get_channel_by_id(self, channel_id: int) -> Optional[Dict[str, Any]]:
        from apps.database.models import Channel
        from apps.database.connection import get_session
        session = get_session()
        try:
            c = session.query(Channel).filter(Channel.id == channel_id).first()
            return self._channel_to_dict(c)
        finally:
            session.close()
    
    def update_channel(self, channel_id: int, channel_data: Dict[str, Any]) -> bool:
        return self.save_channels([channel_data])
    
    # Streams
    def load_streams(self) -> List[Dict[str, Any]]:
        from apps.database.models import Stream
        from apps.database.connection import get_session
        session = get_session()
        try:
            streams = session.query(Stream).all()
            return [self._stream_to_dict(s) for s in streams]
        finally:
            session.close()
    
    def save_streams(self, streams: List[Dict[str, Any]]) -> bool:
        from apps.database.models import Stream
        from apps.database.connection import get_session
        session = get_session()
        try:
            for item in streams:
                sid = item.get('id')
                s = session.query(Stream).filter(Stream.id == sid).first()
                if not s:
                    s = Stream(id=sid)
                    session.add(s)
                
                # Map API fields to database columns, extracting IDs from nested objects
                for k, v in item.items():
                    # Skip relationship fields - only set ID columns
                    if k == 'm3u_account':
                        # API returns dict or int, DB needs m3u_account_id
                        if isinstance(v, dict):
                            v = v.get('id')
                        k = 'm3u_account_id'
                    elif k == 'channel_group':
                        if isinstance(v, dict):
                            v = v.get('id')
                        k = 'channel_group_id'
                    
                    # Only set if column exists in model
                    if hasattr(s, k) and k != 'm3u_account':  # Skip the relationship itself
                        if k in ['updated_at', 'last_seen', 'stats_updated_at'] and isinstance(v, str):
                            try: v = datetime.fromisoformat(v)
                            except: pass
                        setattr(s, k, v)
            
            session.commit()
            self._update_metadata('streams_last_updated')
            logger.info(f"Saved {len(streams)} streams to storage")
            return True
        except Exception as e:
            logger.error(f"Error saving streams to storage: {e}", exc_info=True)
            session.rollback()
            return False
        finally:
            session.close()
    
    def get_stream_by_id(self, stream_id: int) -> Optional[Dict[str, Any]]:
        from apps.database.models import Stream
        from apps.database.connection import get_session
        session = get_session()
        try:
            s = session.query(Stream).filter(Stream.id == stream_id).first()
            return self._stream_to_dict(s)
        finally:
            session.close()
    
    def update_stream(self, stream_id: int, stream_data: Dict[str, Any]) -> bool:
        return self.save_streams([stream_data])
    
    # Channel Groups
    def load_channel_groups(self) -> List[Dict[str, Any]]:
        from apps.database.models import ChannelGroup
        from apps.database.connection import get_session
        session = get_session()
        try:
            groups = session.query(ChannelGroup).all()
            return [self._group_to_dict(g) for g in groups]
        finally:
            session.close()
    
    def save_channel_groups(self, groups: List[Dict[str, Any]]) -> bool:
        from apps.database.models import ChannelGroup
        from apps.database.connection import get_session
        session = get_session()
        try:
            for item in groups:
                gid = item.get('id')
                g = session.query(ChannelGroup).filter(ChannelGroup.id == gid).first()
                if not g:
                    g = ChannelGroup(id=gid)
                    session.add(g)
                for k,v in item.items():
                    if hasattr(g, k): setattr(g, k, v)
            session.commit()
            self._update_metadata('channel_groups_last_updated')
            return True
        except:
            session.rollback()
            return False
        finally:
            session.close()
    
    # Logos
    def load_logos(self) -> List[Dict[str, Any]]:
        from apps.database.models import Logo
        from apps.database.connection import get_session
        session = get_session()
        try:
            logos = session.query(Logo).all()
            return [self._logo_to_dict(l) for l in logos]
        finally:
            session.close()
    
    def save_logos(self, logos: List[Dict[str, Any]]) -> bool:
        from apps.database.models import Logo
        from apps.database.connection import get_session
        session = get_session()
        try:
            for item in logos:
                lid = item.get('id')
                l = session.query(Logo).filter(Logo.id == lid).first()
                if not l:
                    l = Logo(id=lid)
                    session.add(l)
                for k,v in item.items():
                    if hasattr(l, k): setattr(l, k, v)
            session.commit()
            self._update_metadata('logos_last_updated')
            return True
        except:
            session.rollback()
            return False
        finally:
            session.close()
    
    def get_logo_by_id(self, logo_id: int) -> Optional[Dict[str, Any]]:
        from apps.database.models import Logo
        from apps.database.connection import get_session
        session = get_session()
        try:
            l = session.query(Logo).filter(Logo.id == logo_id).first()
            return self._logo_to_dict(l)
        finally:
            session.close()
    
    # M3U Accounts
    def load_m3u_accounts(self) -> List[Dict[str, Any]]:
        from apps.database.models import M3UAccount
        from apps.database.connection import get_session
        session = get_session()
        try:
            accounts = session.query(M3UAccount).all()
            return [self._account_to_dict(a) for a in accounts]
        finally:
            session.close()
    
    def save_m3u_accounts(self, accounts: List[Dict[str, Any]]) -> bool:
        from apps.database.models import M3UAccount
        from apps.database.connection import get_session
        session = get_session()
        try:
            for item in accounts:
                aid = item.get('id')
                a = session.query(M3UAccount).filter(M3UAccount.id == aid).first()
                if not a:
                    a = M3UAccount(id=aid)
                    session.add(a)
                for k,v in item.items():
                    if hasattr(a, k):
                         if k in ['created_at', 'updated_at'] and isinstance(v, str):
                             try: v = datetime.fromisoformat(v)
                             except: pass
                         setattr(a, k, v)
            session.commit()
            self._update_metadata('m3u_accounts_last_updated')
            return True
        except:
            session.rollback()
            return False
        finally:
            session.close()
    
    # Channel Profiles
    def load_channel_profiles(self) -> List[Dict[str, Any]]:
        from apps.database.models import M3UAccountProfile
        from apps.database.connection import get_session
        session = get_session()
        try:
            profiles = session.query(M3UAccountProfile).all()
            return [self._profile_to_dict(p) for p in profiles]
        finally:
            session.close()
    
    def save_channel_profiles(self, profiles: List[Dict[str, Any]]) -> bool:
        from apps.database.models import M3UAccountProfile
        from apps.database.connection import get_session
        session = get_session()
        try:
            for item in profiles:
                pid = item.get('id')
                p = session.query(M3UAccountProfile).filter(M3UAccountProfile.id == pid).first()
                if not p:
                    p = M3UAccountProfile(id=pid)
                    session.add(p)
                for k,v in item.items():
                    if hasattr(p, k): setattr(p, k, v)
            session.commit()
            self._update_metadata('channel_profiles_last_updated')
            return True
        except:
            session.rollback()
            return False
        finally:
            session.close()
    
    # Profile Channels
    def load_profile_channels(self) -> Dict[int, Dict[str, Any]]:
        from apps.database.models import SystemSetting
        from apps.database.connection import get_session
        session = get_session()
        try:
            s = session.query(SystemSetting).filter(SystemSetting.key == 'udi_profile_channels').first()
            if s and s.value:
                return {int(k): v for k,v in s.value.items()}
            return {}
        finally:
            session.close()
    
    def save_profile_channels(self, profile_channels: Dict[int, Dict[str, Any]]) -> bool:
        from apps.database.models import SystemSetting
        from apps.database.connection import get_session
        session = get_session()
        try:
            s = session.query(SystemSetting).filter(SystemSetting.key == 'udi_profile_channels').first()
            if not s:
                s = SystemSetting(key='udi_profile_channels')
                session.add(s)
            s.value = {str(k): v for k,v in profile_channels.items()}
            session.commit()
            self._update_metadata('profile_channels_last_updated')
            return True
        except:
            session.rollback()
            return False
        finally:
            session.close()
    
    def load_profile_channels_by_id(self, profile_id: int) -> Optional[Dict[str, Any]]:
        return self.load_profile_channels().get(profile_id)
    
    def save_profile_channels_by_id(self, profile_id: int, channels_data: Dict[str, Any]) -> bool:
        data = self.load_profile_channels()
        data[profile_id] = channels_data
        return self.save_profile_channels(data)
    
    # Metadata
    def load_metadata(self) -> Dict[str, Any]:
        from apps.database.models import SystemSetting
        from apps.database.connection import get_session
        session = get_session()
        try:
            s = session.query(SystemSetting).filter(SystemSetting.key == 'udi_metadata').first()
            return s.value if s else {}
        finally:
            session.close()
    
    def save_metadata(self, metadata: Dict[str, Any]) -> bool:
        from apps.database.models import SystemSetting
        from apps.database.connection import get_session
        session = get_session()
        try:
            s = session.query(SystemSetting).filter(SystemSetting.key == 'udi_metadata').first()
            if not s:
                s = SystemSetting(key='udi_metadata')
                session.add(s)
            s.value = metadata
            session.commit()
            return True
        except:
            session.rollback()
            return False
        finally:
            session.close()
    
    def _update_metadata(self, field: str) -> None:
        metadata = self.load_metadata()
        metadata[field] = datetime.now().isoformat()
        self.save_metadata(metadata)
    
    def get_last_updated(self, entity_type: str) -> Optional[str]:
        metadata = self.load_metadata()
        return metadata.get(f'{entity_type}_last_updated')
    
    # Match Profiles - Reusing MatchProfilesManager
    def load_match_profiles(self) -> List[Dict[str, Any]]:
        try:
            from apps.automation.match_profiles_manager import get_match_profiles_manager
            return get_match_profiles_manager().get_all_profiles()
        except: return []
    
    def save_match_profiles(self, profiles: List[Dict[str, Any]]) -> bool:
         # Handled by MatchProfilesManager
         return True
    
    def get_match_profile(self, profile_id: int) -> Optional[Dict[str, Any]]:
        try:
            from apps.automation.match_profiles_manager import get_match_profiles_manager
            return get_match_profiles_manager().get_profile(str(profile_id))
        except: return None
    
    def update_match_profile(self, profile_id: int, profile_data: Dict[str, Any]) -> bool:
        try:
            from apps.automation.match_profiles_manager import get_match_profiles_manager
            return get_match_profiles_manager().update_profile(str(profile_id), profile_data)
        except: return False
    
    def delete_match_profile(self, profile_id: int) -> bool:
        try:
            from apps.automation.match_profiles_manager import get_match_profiles_manager
            return get_match_profiles_manager().delete_profile(str(profile_id))
        except: return False
    
    def clear_all(self) -> bool:
        # Avoid clearing main databases unless explicitly asked!
        logger.warning("clear_all invoked on SQL storage, doing nothing for safety")
        return True
    
    def is_initialized(self) -> bool:
        from apps.database.models import Channel
        from apps.database.connection import get_session
        session = get_session()
        try:
            return session.query(Channel).count() > 0
        finally:
            session.close()
