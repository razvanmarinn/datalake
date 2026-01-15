import React from 'react';

const ProjectList = ({ projects, selectedProject, onSelectProject }) => {
  return (
    <div className="md:col-span-1 bg-gray-50 p-4 rounded-xl border h-fit">
      <h2 className="text-xl font-bold mb-4 text-gray-700">
        Your Projects
      </h2>
      <div className="flex flex-col gap-2">
        {Object.entries(projects).map(([projectName, projectID]) => (
          <button
            key={projectName}
            onClick={() => onSelectProject(projectName)}
            className={`text-left px-4 py-3 rounded-lg font-medium transition ${
              selectedProject === projectName
                ? "bg-blue-600 text-white shadow-md"
                : "bg-white hover:bg-blue-50 text-gray-700 border"
            }`}
          >
            {projectName}
          </button>
        ))}
      </div>
    </div>
  );
};

export default ProjectList;
