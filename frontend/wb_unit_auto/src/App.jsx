import React, { useState } from "react";
import Dashboard from "./Dashboard";
import UnitEconomics from "./UnitEconomics";
import MissingCosts from "./MissingCosts";

function App() {
  const [page, setPage] = useState("dashboard");
  const [range, setRange] = useState({ start: "", end: "" });

  return (
    <div>
      {page === "dashboard" && (
        <Dashboard
          openEconomics={() => setPage("economics")}
          openMissing={(r) => {
            setRange(r);
            setPage("missing");
          }}
        />
      )}
      {page === "economics" && (
        <UnitEconomics goBack={() => setPage("dashboard")} />
      )}
      {page === "missing" && (
        <MissingCosts
          startDate={range.start}
          endDate={range.end}
          goBack={() => setPage("dashboard")}
        />
      )}
    </div>
  );
}

export default App;

