import React, { useState } from "react";
import Dashboard from "./Dashboard";
import UnitEconomics from "./UnitEconomics";
import MissingCosts from "./MissingCosts";
import CardChanges from "./CardChanges";
import CardComparison from "./CardComparison";
import ProblemCards from "./ProblemCards";
import ImtDetails from "./ImtDetails";

function App() {
  const [page, setPage] = useState("dashboard");
  const [range, setRange] = useState({ start: "", end: "" });
  const [problemMode, setProblemMode] = useState(null);
  const [imtInfo, setImtInfo] = useState({ id: null, start: "", end: "" });

  return (
    <div>
      {page === "dashboard" && (
        <Dashboard
          openEconomics={() => setPage("economics")}
          openChanges={() => setPage("changes")}
          openCompare={() => setPage("compare")}
          openMissing={(r) => {
            setRange(r);
            setPage("missing");
          }}
          openProblems={(mode, r) => {
            setRange(r);
            setProblemMode(mode);
            setPage("problem");
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
      {page === "changes" && (
        <CardChanges goBack={() => setPage("dashboard")} />
      )}
      {page === "compare" && (
        <CardComparison goBack={() => setPage("dashboard")} />
      )}
      {page === "problem" && (
        <ProblemCards
          mode={problemMode}
          startDate={range.start}
          endDate={range.end}
          goBack={() => setPage("dashboard")}
          openImt={(id) => {
            setImtInfo({ id, start: range.start, end: range.end });
            setPage("imt");
          }}
        />
      )}
      {page === "imt" && (
        <ImtDetails
          imtId={imtInfo.id}
          startDate={imtInfo.start}
          endDate={imtInfo.end}
          goBack={() => setPage("problem")}
        />
      )}
    </div>
  );
}

export default App;

