import { useState, useEffect } from "react";
import axios from "axios";
import { AuthProvider, useAuth } from "./context/AuthContext";
import Login from "./components/Login";
import ProjectList from "./components/ProjectList";
import SchemaBrowser from "./components/SchemaBrowser";
import DataView from "./components/DataView";
import SqlRunner from "./components/SqlRunner";

const Dashboard = () => {
  const { token, username, logout } = useAuth();

  const [projects, setProjects] = useState({});
  const [selectedProject, setSelectedProject] = useState(null);

  const [schemas, setSchemas] = useState([]);
  const [activeSchema, setActiveSchema] = useState(null);
  const [schemaData, setSchemaData] = useState([]);

  const [activeTab, setActiveTab] = useState("browse");

  useEffect(() => {
    if (token && username) {
      fetchProjects();
    }
  }, [token, username]);

  const fetchProjects = async () => {
    try {
      const projResp = await axios.get(
        `http://localhost:8083/meta/projects/by-username/${username}`,
        {
          headers: { Authorization: `Bearer ${token}` },
        },
      );
      setProjects(projResp.data.projects);
    } catch (error) {
      console.error("Failed to fetch projects", error);
    }
  };

  const handleProjectClick = async (projectName) => {
    setSelectedProject(projectName);
    setSchemas([]);
    setActiveSchema(null);
    setSchemaData([]);
    setActiveTab("browse");

    try {
      const schemaResp = await axios.get(
        `http://localhost:8083/meta/${projectName}/schemas`,
        {
          headers: { Authorization: `Bearer ${token}` },
        },
      );
      setSchemas(schemaResp.data);
    } catch (error) {
      console.error("Failed to fetch schemas", error);
      alert("Failed to fetch schemas for " + projectName);
    }
  };

  const handleViewData = async (schemaName) => {
    setActiveSchema(schemaName);
    setSchemaData([]);

    try {
      const response = await axios.get(
        `http://localhost:8083/query/projects/${selectedProject}/schemas/${schemaName}/data`,
        {
          headers: { Authorization: `Bearer ${token}` },
        },
      );
      setSchemaData(response.data);
    } catch (error) {
      console.error("Failed to fetch schema data", error);
      alert("Failed to fetch data for " + schemaName);
    }
  };

  if (!token) {
    return <Login />;
  }

  return (
    <div className="min-h-screen bg-gray-50 p-8">
      <div className="max-w-7xl mx-auto bg-white p-6 rounded-2xl shadow">
        {/* Header */}
        <div className="flex justify-between items-center mb-8 border-b pb-4">
          <div>
            <h1 className="text-3xl font-bold text-gray-800">Data Catalog</h1>
            <p className="text-gray-500">Welcome, {username}</p>
          </div>
          <button
            onClick={() => {
              logout();
              setProjects({});
              setSchemas([]);
              setSelectedProject(null);
            }}
            className="bg-red-500 hover:bg-red-600 text-white font-semibold py-2 px-6 rounded transition"
          >
            Logout
          </button>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-4 gap-8">
          {/* Sidebar: Projects */}
          <ProjectList
            projects={projects}
            selectedProject={selectedProject}
            onSelectProject={handleProjectClick}
          />

          {/* Main Content Area */}
          <div className="md:col-span-3">
            {selectedProject ? (
              <>
                {/* Tabs */}
                <div className="flex space-x-4 mb-6 border-b">
                  <button
                    onClick={() => setActiveTab("browse")}
                    className={`pb-2 px-4 font-semibold transition ${
                      activeTab === "browse"
                        ? "text-blue-600 border-b-2 border-blue-600"
                        : "text-gray-500 hover:text-gray-700"
                    }`}
                  >
                    Browse Data
                  </button>
                  <button
                    onClick={() => setActiveTab("sql")}
                    className={`pb-2 px-4 font-semibold transition ${
                      activeTab === "sql"
                        ? "text-blue-600 border-b-2 border-blue-600"
                        : "text-gray-500 hover:text-gray-700"
                    }`}
                  >
                    SQL Runner
                  </button>
                </div>

                {/* Tab: Browse Data */}
                {activeTab === "browse" && (
                  <>
                    {!activeSchema ? (
                      <div className="mb-8 animate-fade-in">
                        <h2 className="text-2xl font-bold mb-4 text-gray-800">
                          Schemas for{" "}
                          <span className="text-blue-600">
                            {selectedProject}
                          </span>
                        </h2>
                        <SchemaBrowser
                          schemas={schemas}
                          onViewData={handleViewData}
                        />
                      </div>
                    ) : (
                      <DataView
                        schemaName={activeSchema}
                        data={schemaData}
                        onBack={() => setActiveSchema(null)}
                      />
                    )}
                  </>
                )}

                {/* Tab: SQL Runner */}
                {activeTab === "sql" && (
                  <SqlRunner
                    selectedProject={selectedProject}
                    projectId={projects[selectedProject]}
                  />
                )}
              </>
            ) : (
              <div className="h-full flex items-center justify-center bg-gray-50 border-2 border-dashed border-gray-200 rounded-xl p-12">
                <div className="text-center">
                  <h3 className="mt-2 text-sm font-semibold text-gray-900">
                    No project selected
                  </h3>
                  <p className="mt-1 text-sm text-gray-500">
                    Select a project from the sidebar to view schemas or run
                    queries.
                  </p>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

function App() {
  return (
    <AuthProvider>
      <Dashboard />
    </AuthProvider>
  );
}

export default App;
