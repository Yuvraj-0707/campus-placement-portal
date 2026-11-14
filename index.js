import dns from "node:dns";
dns.setServers(["8.8.8.8", "8.8.4.4"]);

import express from "express";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { MongoClient } from "mongodb";
import dotenv from "dotenv";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const port = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const uri = process.env.MONGO_URI;
if (!uri) {
  console.error("Please define MONGO_URI in your environment variables or .env file!");
  process.exit(1);
}

const client = new MongoClient(uri);
let db;

async function getDB() {
  if (!db) {
    await client.connect();
    db = client.db("campus_placement_db");
    console.log("Connected to MongoDB Atlas successfully.");
  }
  return db;
}

// Ensure DB connection is active before handling any /api route
app.use("/api", async (req, res, next) => {
  try {
    await getDB();
    next();
  } catch (err) {
    res.status(500).json({ error: "Database connection failed: " + err.message });
  }
});

// ==========================================
// 1. GET ALL STUDENTS
// ==========================================
app.get("/api/students", async (req, res) => {
  try {
    const students = await db.collection("students").find().toArray();
    res.json(students);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ==========================================
// 2. GET AGGREGATION PIPELINE REPORT
// ==========================================
app.get("/api/report", async (req, res) => {
  try {
    const job = await db.collection("jobs").findOne();
    if (!job) return res.json([]);

    const report = await db.collection("applications").aggregate([
      { $match: { jobId: job._id } },
      {
        $lookup: {
          from: "students",
          localField: "studentId",
          foreignField: "_id",
          as: "studentInfo"
        }
      },
      { $unwind: "$studentInfo" },
      {
        $project: {
          _id: 0,
          candidate: { $concat: ["$studentInfo.firstName", " ", "$studentInfo.lastName"] },
          rollNumber: "$studentInfo.rollNumber",
          branch: "$studentInfo.branch",
          currentStage: "$applicationStatus",
          roundsCleared: {
            $size: {
              $filter: {
                input: { $ifNull: ["$evaluations", []] },
                as: "ev",
                cond: { $eq: ["$$ev.resultStatus", "Qualified"] }
              }
            }
          },
          avgScore: { $round: [{ $avg: "$evaluations.evaluationScore" }, 2] },
          hasOffer: { $cond: [{ $ne: ["$offer", null] }, "YES", "NO"] }
        }
      }
    ]).toArray();

    res.json(report);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ==========================================
// 3. MULTI-DOCUMENT ACID TRANSACTION ENDPOINT
// ==========================================
app.post("/api/accept-offer", async (req, res) => {
  const session = client.startSession();
  try {
    session.startTransaction();

    const riya = await db.collection("students").findOne({ rollNumber: "2023IT0204" }, { session });
    if (!riya) {
      await session.abortTransaction();
      return res.status(404).json({ message: "Candidate record not found. Click 'Reset & Seed Database' first." });
    }

    const appDoc = await db.collection("applications").findOne(
      { studentId: riya._id, "offer.acceptanceStatus": "Pending" },
      { session }
    );

    if (!appDoc) {
      await session.abortTransaction();
      return res.status(400).json({ message: "No pending offer found or offer already accepted." });
    }

    // 1. Accept offer on application
    await db.collection("applications").updateOne(
      { _id: appDoc._id },
      { $set: { "offer.acceptanceStatus": "Accepted", applicationStatus: "Placed" } },
      { session }
    );

    // 2. Lock student placement status
    await db.collection("students").updateOne(
      { _id: riya._id },
      { $set: { placementStatus: "Placed" } },
      { session }
    );

    // 3. Mark competing applications as Rejected
    await db.collection("applications").updateMany(
      { studentId: riya._id, _id: { $ne: appDoc._id } },
      { $set: { applicationStatus: "Rejected" } },
      { session }
    );

    await session.commitTransaction();
    const updatedStudent = await db.collection("students").findOne({ _id: riya._id });
    res.json({ message: "ACID Transaction committed successfully!", student: updatedStudent });
  } catch (err) {
    await session.abortTransaction();
    res.status(500).json({ error: err.message });
  } finally {
    await session.endSession();
  }
});

// ==========================================
// 4. RESET & SEED ENDPOINT (WITH SCHEMA & INDEXES)
// ==========================================
app.post("/api/seed", async (req, res) => {
  try {
    const existing = await db.listCollections().toArray();
    const names = existing.map(c => c.name);

    for (const name of ["applications", "jobs", "companies", "students"]) {
      if (names.includes(name)) await db.collection(name).drop();
    }

    // Recreate collections with $jsonSchema validation
    await db.createCollection("students", {
      validator: {
        $jsonSchema: {
          bsonType: "object",
          required: ["rollNumber", "firstName", "lastName", "email", "branch", "cgpa", "activeBacklogs", "placementStatus"],
          properties: {
            rollNumber: { bsonType: "string" },
            firstName: { bsonType: "string" },
            lastName: { bsonType: "string" },
            email: { bsonType: "string", pattern: "^.+@.+\\..+$" },
            branch: { bsonType: "string" },
            cgpa: { bsonType: ["double", "int", "number"], minimum: 0.0, maximum: 10.0 },
            activeBacklogs: { bsonType: ["int", "number"], minimum: 0 },
            placementStatus: { enum: ["Unplaced", "Placed", "Opted-Out"] }
          }
        }
      }
    });

    await db.createCollection("companies", {
      validator: {
        $jsonSchema: {
          bsonType: "object",
          required: ["companyName", "industrySector", "hrEmail"],
          properties: {
            companyName: { bsonType: "string" },
            industrySector: { bsonType: "string" },
            hrEmail: { bsonType: "string", pattern: "^.+@.+\\..+$" }
          }
        }
      }
    });

    await db.createCollection("jobs", {
      validator: {
        $jsonSchema: {
          bsonType: "object",
          required: ["companyId", "jobTitle", "packageCTC", "minCGPA", "maxBacklogsAllowed", "eligibleBranches", "status"],
          properties: {
            companyId: { bsonType: "objectId" },
            jobTitle: { bsonType: "string" },
            packageCTC: { bsonType: ["double", "int", "number"], minimum: 0.0 },
            minCGPA: { bsonType: ["double", "int", "number"], minimum: 0.0, maximum: 10.0 },
            maxBacklogsAllowed: { bsonType: ["int", "number"], minimum: 0 },
            eligibleBranches: { bsonType: "array", items: { bsonType: "string" } },
            status: { enum: ["Open", "Closed"] }
          }
        }
      }
    });

    await db.createCollection("applications", {
      validator: {
        $jsonSchema: {
          bsonType: "object",
          required: ["studentId", "jobId", "appliedAt", "applicationStatus"],
          properties: {
            studentId: { bsonType: "objectId" },
            jobId: { bsonType: "objectId" },
            appliedAt: { bsonType: "date" },
            applicationStatus: { enum: ["Applied", "Shortlisted", "Rejected", "Offered", "Placed"] }
          }
        }
      }
    });

    // Enforce unique constraints
    await db.collection("students").createIndex({ rollNumber: 1 }, { unique: true });
    await db.collection("students").createIndex({ email: 1 }, { unique: true });
    await db.collection("applications").createIndex({ studentId: 1, jobId: 1 }, { unique: true });

    const studentsCol = db.collection("students");
    const companiesCol = db.collection("companies");
    const jobsCol = db.collection("jobs");
    const applicationsCol = db.collection("applications");

    const studentResult = await studentsCol.insertMany([
      {
        rollNumber: "2023CS0101",
        firstName: "Aman",
        lastName: "Verma",
        email: "aman.v@univ.edu",
        branch: "Computer Science",
        cgpa: 8.85,
        activeBacklogs: 0,
        placementStatus: "Unplaced"
      },
      {
        rollNumber: "2023IT0204",
        firstName: "Riya",
        lastName: "Sharma",
        email: "riya.s@univ.edu",
        branch: "Information Technology",
        cgpa: 9.30,
        activeBacklogs: 0,
        placementStatus: "Unplaced"
      },
      {
        rollNumber: "2023EC0310",
        firstName: "Kunal",
        lastName: "Singh",
        email: "kunal.s@univ.edu",
        branch: "Electronics",
        cgpa: 7.20,
        activeBacklogs: 1,
        placementStatus: "Unplaced"
      }
    ]);

    const compResult = await companiesCol.insertOne({
      companyName: "NexTech Systems",
      industrySector: "Cloud & Distributed Systems",
      hrEmail: "recruiting@nextech.example.com"
    });

    const jobResult = await jobsCol.insertOne({
      companyId: compResult.insertedId,
      jobTitle: "Software Development Engineer - I",
      packageCTC: 18.5,
      minCGPA: 8.0,
      maxBacklogsAllowed: 0,
      eligibleBranches: ["Computer Science", "Information Technology"],
      status: "Open"
    });

    await applicationsCol.insertMany([
      {
        studentId: studentResult.insertedIds[0],
        jobId: jobResult.insertedId,
        appliedAt: new Date(),
        applicationStatus: "Shortlisted",
        evaluations: [
          { roundNumber: 1, evaluationScore: 89.0, resultStatus: "Qualified" },
          { roundNumber: 2, evaluationScore: 78.5, resultStatus: "Qualified" }
        ]
      },
      {
        studentId: studentResult.insertedIds[1],
        jobId: jobResult.insertedId,
        appliedAt: new Date(),
        applicationStatus: "Offered",
        evaluations: [
          { roundNumber: 1, evaluationScore: 97.0, resultStatus: "Qualified" },
          { roundNumber: 2, evaluationScore: 94.0, resultStatus: "Qualified" },
          { roundNumber: 3, evaluationScore: 90.0, resultStatus: "Qualified" }
        ],
        offer: {
          offerDate: new Date(),
          baseSalary: 16.0,
          acceptanceStatus: "Pending"
        }
      }
    ]);

    res.json({ message: "Database re-seeded with schema validation and indexes." });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Fallback route to serve index.html for root requests
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// Run locally when not in Vercel production
if (process.env.NODE_ENV !== "production") {
  getDB().then(() => {
    app.listen(port, () => {
      console.log(`CPIMP Server running at: http://localhost:${port}`);
    });
  });
}

// Export Express app for Vercel Serverless
export default app;