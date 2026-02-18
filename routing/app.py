import streamlit as st
import time
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="ORS Profile Manager",
    page_icon="🗺️",
    layout="wide"
)

session = get_active_session()

st.title("🗺️ OpenRouteService Profile Manager")
st.markdown("Manage and switch between different routing profiles")

st.divider()

@st.cache_data(ttl=30)
def get_available_profiles():
    """Get all available profiles from the stage"""
    try:
        result = session.sql("""
            LIST @OPENROUTESERVICE_NATIVE_APP.CORE.ORS_SPCS_STAGE
        """).collect()
        
        profiles = set()
        for row in result:
            name = row['name']
            if '/' in name:
                profile_name = name.split('/')[1]
                if profile_name and profile_name not in ['Notebook']:
                    profiles.add(profile_name)
        
        return sorted(list(profiles))
    except Exception as e:
        st.error(f"Error fetching profiles: {e}")
        return []

@st.cache_data(ttl=30)
def get_profile_details(profile_name):
    """Get details about a specific profile"""
    try:
        result = session.sql(f"""
            LIST @OPENROUTESERVICE_NATIVE_APP.CORE.ORS_SPCS_STAGE/{profile_name}/
        """).collect()
        
        details = {
            'map_file': None,
            'map_size': 0,
            'config_exists': False
        }
        
        for row in result:
            name = row['name']
            if name.endswith('.osm.pbf') and 'example' not in name.lower():
                details['map_file'] = name.split('/')[-1]
                details['map_size'] = row['size']
            elif name.endswith('ors-config.yml'):
                details['config_exists'] = True
        
        return details
    except Exception as e:
        return None

def get_service_status():
    """Get the status of ORS service"""
    try:
        result = session.sql("""
            SHOW SERVICES LIKE 'ORS_SERVICE' IN SCHEMA OPENROUTESERVICE_NATIVE_APP.CORE
        """).collect()
        
        if result:
            return result[0]['status']
        return 'UNKNOWN'
    except Exception as e:
        st.error(f"Error fetching service status: {e}")
        return 'ERROR'

def get_ors_initialization_status():
    """Check if ORS is fully initialized by examining logs"""
    try:
        result = session.sql("""
            CALL SYSTEM$GET_SERVICE_LOGS('OPENROUTESERVICE_NATIVE_APP.CORE.ORS_SERVICE', 0, 'ors', 1000)
        """).collect()
        
        if result and result[0]:
            logs = str(result[0][0])
            
            status = {
                'is_ready': False,
                'stage': 'Unknown',
                'details': '',
                'profiles_loaded': 0,
                'total_profiles': 3
            }
            
            if 'Started Application' in logs or 'Tomcat started' in logs:
                status['is_ready'] = True
                status['stage'] = 'Ready'
                status['details'] = 'All profiles loaded, service is ready'
                status['profiles_loaded'] = 3
                return status
            
            if 'Initializing' in logs and 'profiles' in logs:
                status['stage'] = 'Loading'
                
                # Check if profiles finished loading
                if 'driving-car' in logs and 'Finished at:' in logs:
                    status['profiles_loaded'] += 1
                if 'driving-hgv' in logs and 'Finished at:' in logs:
                    status['profiles_loaded'] += 1
                if 'cycling-road' in logs and 'Finished at:' in logs:
                    status['profiles_loaded'] += 1
                
                # Check for graph loading indicators (means using existing graphs)
                if 'Total time:' in logs and status['profiles_loaded'] > 0:
                    status['details'] = f"Loading existing graphs: {status['profiles_loaded']}/3 profiles complete"
                elif status['profiles_loaded'] > 0:
                    status['details'] = f"Loading graphs: {status['profiles_loaded']}/3 profiles complete"
                else:
                    status['details'] = 'Initializing profiles...'
                return status
            
            if 'Container file system preparation' in logs:
                status['stage'] = 'Preparing'
                status['details'] = 'Initializing container...'
                return status
            
            if 'Container ENV' in logs or 'Container sanity checks' in logs:
                status['stage'] = 'Starting'
                status['details'] = 'Container starting up...'
                return status
            
            return status
            
    except Exception as e:
        return {
            'is_ready': False,
            'stage': 'Error',
            'details': str(e),
            'profiles_loaded': 0,
            'total_profiles': 3
        }

def get_all_services_status():
    """Get the status of all routing services"""
    try:
        result = session.sql("""
            SHOW SERVICES IN SCHEMA OPENROUTESERVICE_NATIVE_APP.CORE
        """).collect()
        
        services = {}
        for row in result:
            services[row['name']] = {
                'status': row['status'],
                'instances': f"{row['current_instances']}/{row['target_instances']}"
            }
        return services
    except Exception as e:
        st.error(f"Error fetching services: {e}")
        return {}

def get_active_profile_from_logs():
    """Determine active profile from actual service specification (ground truth)"""
    try:
        result = session.sql("""
            DESC SERVICE OPENROUTESERVICE_NATIVE_APP.CORE.ORS_SERVICE
        """).collect()
        
        if result and len(result) > 0:
            spec = str(result[0]['spec'])
            
            if '@OPENROUTESERVICE_NATIVE_APP.CORE.ORS_SPCS_STAGE/USA' in spec or '@OPENROUTESERVICE_NATIVE_APP.CORE.ORS_GRAPHS_SPCS_STAGE/USA' in spec:
                return 'USA'
            elif '@OPENROUTESERVICE_NATIVE_APP.CORE.ORS_SPCS_STAGE/germany' in spec or '@OPENROUTESERVICE_NATIVE_APP.CORE.ORS_GRAPHS_SPCS_STAGE/germany' in spec:
                return 'germany'
            elif '@OPENROUTESERVICE_NATIVE_APP.CORE.ORS_SPCS_STAGE/SanFrancisco' in spec or '@OPENROUTESERVICE_NATIVE_APP.CORE.ORS_GRAPHS_SPCS_STAGE/SanFrancisco' in spec:
                return 'SanFrancisco'
        
        return 'Unknown'
    except Exception as e:
        st.warning(f"Could not determine active profile from service spec: {e}")
        return 'Unknown'

def check_graphs_exist(profile_name):
    """Check if pre-built graphs exist for a profile in the graphs stage"""
    try:
        result = session.sql(f"""
            LIST @OPENROUTESERVICE_NATIVE_APP.CORE.ORS_GRAPHS_SPCS_STAGE/{profile_name}/
        """).collect()
        # Check if there are any graph files (multiple directories like driving-car, etc)
        return len(result) > 0
    except:
        return False

def switch_profile(target_profile):
    """Switch to a different profile using existing graphs if available"""
    
    with st.spinner(f"🔄 Switching to **{target_profile}** profile..."):
        try:
            graphs_exist = check_graphs_exist(target_profile)
            
            if graphs_exist:
                st.info(f"✅ Found existing graphs for {target_profile} - will use pre-built graphs")
                rebuild_needed = False
            else:
                st.warning(f"⚠️ No existing graphs found for {target_profile} - graphs will need to be built (5-10 min)")
                rebuild_needed = True
            
            st.info("⏸️ Suspending services...")
            session.sql("""
                ALTER SERVICE OPENROUTESERVICE_NATIVE_APP.CORE.ORS_SERVICE SUSPEND
            """).collect()
            
            session.sql("""
                ALTER SERVICE OPENROUTESERVICE_NATIVE_APP.CORE.ROUTING_GATEWAY_SERVICE SUSPEND
            """).collect()
            
            time.sleep(3)
            
            # Update ORS service specification to point to new profile
            st.info(f"🔄 Updating service to use {target_profile} profile...")
            
            new_spec = f"""spec:
  containers:
  - name: ors
    image: /openrouteservice_setup/public/image_repository/openrouteservice:v9.0.0
    volumeMounts:
      - name: files
        mountPath: /home/ors/files
      - name: graphs
        mountPath: /home/ors/graphs
      - name: elevation-cache
        mountPath: /home/ors/elevation_cache
    env:
      REBUILD_GRAPHS: "false"
      ORS_CONFIG_LOCATION: /home/ors/files/ors-config.yml
      XMS: 3G 
      XMX: 200G
  endpoints:
    - name: ors
      port: 8082
      public: false
  volumes:
    - name: files
      source: "@CORE.ORS_SPCS_STAGE/{target_profile}"
    - name: graphs
      source: "@CORE.ORS_GRAPHS_SPCS_STAGE/{target_profile}"
    - name: elevation-cache
      source: "@CORE.ORS_ELEVATION_CACHE_SPCS_STAGE/{target_profile}"
"""
            
            session.sql(f"""
                ALTER SERVICE OPENROUTESERVICE_NATIVE_APP.CORE.ORS_SERVICE
                FROM SPECIFICATION $$
{new_spec}
$$
            """).collect()
            
            st.info("▶️ Resuming services with new profile...")
            session.sql("""
                ALTER SERVICE OPENROUTESERVICE_NATIVE_APP.CORE.ROUTING_GATEWAY_SERVICE RESUME
            """).collect()
            
            session.sql("""
                ALTER SERVICE OPENROUTESERVICE_NATIVE_APP.CORE.ORS_SERVICE RESUME
            """).collect()
            
            if rebuild_needed:
                st.info("📥 Triggering graph build...")
                time.sleep(5)
                try:
                    session.sql("""
                        CALL OPENROUTESERVICE_NATIVE_APP.CORE.START_DOWNLOADER()
                    """).collect()
                except:
                    st.warning("Note: Graphs may need to be built on first access")
            
            st.info("⏳ Waiting for services to initialize...")
            time.sleep(10)
            
            if graphs_exist:
                st.success(f"✅ Successfully switched to **{target_profile}** profile using existing graphs!")
                st.info("Services should be ready in ~1 minute")
            else:
                st.success(f"✅ Successfully initiated switch to **{target_profile}** profile!")
                st.info("New graphs are being built. This may take 5-10 minutes.")
            
            st.balloons()
            
            get_available_profiles.clear()
            get_profile_details.clear()
            
            time.sleep(2)
            st.experimental_rerun()
            
        except Exception as e:
            st.error(f"❌ Error switching profile: {e}")
            st.info("Resuming services...")
            try:
                session.sql("""
                    ALTER SERVICE OPENROUTESERVICE_NATIVE_APP.CORE.ORS_SERVICE RESUME
                """).collect()
                session.sql("""
                    ALTER SERVICE OPENROUTESERVICE_NATIVE_APP.CORE.ROUTING_GATEWAY_SERVICE RESUME
                """).collect()
            except:
                pass

col1, col2 = st.columns([2, 1])

with col2:
    st.subheader("⚙️ Services Status")
    
    all_services = get_all_services_status()
    ors_init_status = get_ors_initialization_status()
    
    status_emoji = {
        'RUNNING': '🟢',
        'READY': '🟢',
        'PENDING': '🟡',
        'SUSPENDED': '🔴',
        'UNKNOWN': '⚪',
        'ERROR': '⚠️'
    }
    
    st.markdown("**ORS Engine Status**")
    if ors_init_status['is_ready']:
        st.success("✅ Ready - All graphs loaded")
    else:
        stage = ors_init_status['stage']
        if stage == 'Loading':
            progress = ors_init_status['profiles_loaded'] / ors_init_status['total_profiles']
            st.warning(f"🔄 {stage} - {ors_init_status['details']}")
            st.progress(progress)
        elif stage == 'Preparing' or stage == 'Starting':
            st.info(f"⏳ {stage} - {ors_init_status['details']}")
        else:
            st.warning(f"⚠️ {stage} - {ors_init_status['details']}")
    
    st.markdown("---")
    st.markdown("**Service Containers**")
    
    if all_services:
        for service_name in ['ORS_SERVICE', 'ROUTING_GATEWAY_SERVICE', 'VROOM_SERVICE', 'DOWNLOADER']:
            if service_name in all_services:
                service = all_services[service_name]
                status = service['status']
                instances = service['instances']
                
                col_name, col_status = st.columns([2, 1])
                with col_name:
                    st.write(f"**{service_name.replace('_', ' ').title()}**")
                    st.caption(f"Instances: {instances}")
                with col_status:
                    st.write(f"{status_emoji.get(status, '⚪')} {status}")
                
                if service_name != 'DOWNLOADER':
                    st.markdown("")
    else:
        service_status = get_service_status()
        st.metric(
            label="ORS Service",
            value=service_status,
            delta=status_emoji.get(service_status, '⚪')
        )
    
    st.markdown("---")
    
    active_profile = get_active_profile_from_logs()
    
    if active_profile:
        st.subheader("📊 Active Profile")
        st.markdown("---")
        st.markdown(f"### ✅ {active_profile}")
        
        details = get_profile_details(active_profile)
        if details and details['map_size'] > 0:
            size_mb = details['map_size'] / (1024 * 1024)
            if size_mb > 1024:
                size_str = f"{size_mb/1024:.2f} GB"
            else:
                size_str = f"{size_mb:.2f} MB"
            st.write(f"**Map:** {details['map_file']}")
            st.write(f"**Size:** {size_str}")
    
    st.markdown("---")
    
    if st.button("🔄 Refresh Status", use_container_width=True):
        get_available_profiles.clear()
        get_profile_details.clear()
        st.experimental_rerun()
    
    with st.expander("ℹ️ About"):
        st.markdown("""
        **How to switch:**
        1. Select an inactive profile
        2. Click "Activate" button
        3. Wait for service to restart
        
        **Graph loading time:**
        - Small (SF): 1-2 minutes
        - Large (Germany): 5-10 minutes
        """)

with col1:
    st.subheader("📍 Available Profiles")
    
    profiles = get_available_profiles()
    active_profile = get_active_profile_from_logs()
    
    if profiles:
        for profile in profiles:
            is_active = (profile == active_profile)
            details = get_profile_details(profile)
            
            st.markdown("---")
            profile_col1, profile_col2, profile_col3 = st.columns([3, 3, 2])
            
            with profile_col1:
                if is_active:
                    st.markdown(f"### ✅ **{profile}**")
                    st.caption("Currently Active")
                else:
                    st.markdown(f"### {profile}")
            
            with profile_col2:
                if details and details['map_size'] > 0:
                    size_mb = details['map_size'] / (1024 * 1024)
                    if size_mb > 1024:
                        size_str = f"{size_mb/1024:.2f} GB"
                    else:
                        size_str = f"{size_mb:.2f} MB"
                    
                    st.write(f"📦 **Map File**")
                    st.caption(details['map_file'])
                    
                    st.write(f"💾 **Size**")
                    st.caption(size_str)
            
            with profile_col3:
                st.write("")
                st.write("")
                if not is_active:
                    if st.button(
                        "Activate", 
                        key=f"activate_{profile}", 
                        type="primary",
                        use_container_width=True
                    ):
                        switch_profile(profile)
                else:
                    st.button(
                        "Active",
                        key=f"active_{profile}",
                        disabled=True,
                        use_container_width=True
                    )
    else:
        st.warning("No profiles found in the ORS stage.")

st.markdown("---")
st.caption("🗺️ Profile Manager for OpenRouteService Native App")
