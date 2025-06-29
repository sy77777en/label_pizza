import streamlit as st
import streamlit.components.v1 as components

st.title("🧪 Scroll Test with st.components.v1.html")

# Add content to make page long
st.markdown("## 🏠 Top Section")
for i in range(8):
    st.markdown(f"### Section {i+1}")
    st.write(f"This is section {i+1} content to make the page long...")
    if i < 7:
        st.markdown("---")

# Target section (what we want to scroll to)
st.markdown('<div id="video-list-section"></div>', unsafe_allow_html=True)
st.markdown("## 🎯 TARGET: Video List Section")
st.success("✅ This is the target section we want to scroll to!")

# Add more content below
for i in range(10):
    st.markdown(f"**Video {i+1}:** Sample video content here...")
    if i < 9:
        st.markdown("---")

# Add more content to test scrolling
st.markdown("## 📊 Bottom Content")
for i in range(5):
    st.markdown(f"### Footer Section {i+1}")
    st.write("More content at the bottom...")

st.markdown("---")

# Test the component-based scroll solution
st.markdown("### 🧪 Test the Component Solution")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("**Method 1: Basic parent.document**")
    components.html("""
    <div style="text-align: center; padding: 10px;">
        <a href="#" onclick="
            parent.document.getElementById('video-list-section').scrollIntoView({behavior: 'smooth'});
            return false;
        " style="color: #9553FE; text-decoration: none; font-weight: bold; padding: 8px 12px; border: 2px solid #9553FE; border-radius: 6px;">
            🎯 Scroll to Target
        </a>
    </div>
    """, height=60)

with col2:
    st.markdown("**Method 2: With error handling**")
    components.html("""
    <div style="text-align: center; padding: 10px;">
        <a href="#" onclick="
            try {
                var element = parent.document.getElementById('video-list-section');
                if (element) {
                    element.scrollIntoView({behavior: 'smooth'});
                } else {
                    console.log('Element not found');
                }
            } catch (e) {
                console.log('Error:', e);
            }
            return false;
        " style="color: #e74c3c; text-decoration: none; font-weight: bold; padding: 8px 12px; border: 2px solid #e74c3c; border-radius: 6px;">
            🛡️ Safe Scroll
        </a>
    </div>
    """, height=60)

with col3:
    st.markdown("**Method 3: Instant scroll**")
    components.html("""
    <div style="text-align: center; padding: 10px;">
        <a href="#" onclick="
            parent.document.getElementById('video-list-section').scrollIntoView();
            return false;
        " style="color: #f39c12; text-decoration: none; font-weight: bold; padding: 8px 12px; border: 2px solid #f39c12; border-radius: 6px;">
            ⚡ Instant Scroll
        </a>
    </div>
    """, height=60)

# Test different target elements
st.markdown("### 🎯 Test Different Targets")

col4, col5 = st.columns(2)

with col4:
    st.markdown("**Scroll to page top**")
    components.html("""
    <div style="text-align: center; padding: 10px;">
        <a href="#" onclick="
            parent.window.scrollTo({top: 0, behavior: 'smooth'});
            return false;
        " style="color: #2ecc71; text-decoration: none; font-weight: bold; padding: 8px 12px; border: 2px solid #2ecc71; border-radius: 6px;">
            ⬆️ Top of Page
        </a>
    </div>
    """, height=60)

with col5:
    st.markdown("**Scroll to page bottom**")
    components.html("""
    <div style="text-align: center; padding: 10px;">
        <a href="#" onclick="
            parent.window.scrollTo({top: parent.document.body.scrollHeight, behavior: 'smooth'});
            return false;
        " style="color: #8e44ad; text-decoration: none; font-weight: bold; padding: 8px 12px; border: 2px solid #8e44ad; border-radius: 6px;">
            ⬇️ Bottom of Page
        </a>
    </div>
    """, height=60)

# Instructions
st.markdown("---")
st.markdown("### 📋 Test Instructions:")
st.markdown("""
1. **Scroll down** to the bottom of this page
2. **Click the test buttons** above to see if they scroll to the target section
3. **Check the URL** - it should NOT change when you click the buttons
4. **Check browser console** (F12) for any error messages
5. **Test all methods** to see which one works best

**Expected behavior:**
- ✅ Smooth scroll to the target section
- ✅ URL remains clean (no hash)
- ✅ No page refresh
- ✅ Console shows no errors
""")

# URL monitoring
st.markdown("### 🔍 URL Monitoring")
st.info("Current URL will appear here - watch to see if it changes when you click the scroll buttons")

# JavaScript to monitor URL changes
components.html("""
<div id="url-display" style="padding: 10px; background: #f0f0f0; border-radius: 6px; font-family: monospace;">
    Loading URL...
</div>

<script>
function updateURL() {
    var urlDisplay = document.getElementById('url-display');
    if (urlDisplay) {
        urlDisplay.innerHTML = 'Current URL: ' + parent.window.location.href;
    }
}

// Update URL display every 500ms
setInterval(updateURL, 500);
updateURL(); // Initial call
</script>
""", height=80)