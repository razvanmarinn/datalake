import React from 'react';

const DataView = ({ schemaName, data, onBack }) => {
  const getDataHeaders = () => {
    if (!data || data.length === 0) return [];
    return Object.keys(data[0]);
  };

  return (
    <div className="mb-8">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-2xl font-bold text-gray-800">
          Data:{" "}
          <span className="text-green-600">
            {schemaName}
          </span>
        </h2>
        <button
          onClick={onBack}
          className="text-gray-500 hover:text-gray-700 underline"
        >
          ← Back to Schemas
        </button>
      </div>

      {data.length > 0 ? (
        <div className="overflow-x-auto border rounded-lg shadow-sm max-h-[500px] overflow-y-auto">
          <table className="w-full text-sm text-left text-gray-500">
            <thead className="text-xs text-gray-700 uppercase bg-gray-100 sticky top-0">
              <tr>
                {getDataHeaders().map((header) => (
                  <th
                    key={header}
                    className="px-6 py-3 border-b"
                  >
                    {header}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.map((row, rowIndex) => (
                <tr
                  key={rowIndex}
                  className="bg-white border-b hover:bg-gray-50"
                >
                  {getDataHeaders().map((header) => (
                    <td
                      key={`${rowIndex}-${header}`}
                      className="px-6 py-4"
                    >
                      {typeof row[header] === "object"
                        ? JSON.stringify(row[header])
                        : row[header]}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="p-8 text-center bg-gray-50 rounded-lg border border-dashed border-gray-300">
          <p className="text-gray-500">
            No data found for this schema (or loading...)
          </p>
        </div>
      )}
    </div>
  );
};

export default DataView;
