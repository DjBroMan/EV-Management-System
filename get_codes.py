import os

# Folder containing your Java project
PROJECT_FOLDER = "."

# Output text file
OUTPUT_FILE = "all_java_codes.txt"


def collect_java_files(folder):
    java_files = []

    for root, dirs, files in os.walk(folder):
        # Don't include compiled files or the output log/bin folders
        dirs[:] = [
            d for d in dirs
            if d not in {"bin", "log", ".git"}
        ]

        for file in files:
            if file.endswith(".java"):
                java_files.append(os.path.join(root, file))

    return sorted(java_files)


def main():

    java_files = collect_java_files(PROJECT_FOLDER)

    if not java_files:
        print("No Java files found.")
        return

    with open(
        OUTPUT_FILE,
        "w",
        encoding="utf-8"
    ) as output:

        output.write(
            "============================================================\n"
        )
        output.write(
            "             EV CHARGING NETWORK - JAVA CODES\n"
        )
        output.write(
            "============================================================\n\n"
        )

        for file_path in java_files:

            # Get only the filename
            file_name = os.path.basename(file_path)

            output.write(
                "============================================================\n"
            )
            output.write(
                f"{file_name}\n"
            )
            output.write(
                "============================================================\n\n"
            )

            try:
                with open(
                    file_path,
                    "r",
                    encoding="utf-8"
                ) as source:

                    code = source.read()

                output.write(code)

            except UnicodeDecodeError:
                # Fallback for files saved with another encoding
                with open(
                    file_path,
                    "r",
                    encoding="latin-1"
                ) as source:

                    code = source.read()

                output.write(code)

            output.write("\n\n")

    print("Done!")
    print(f"Java files found: {len(java_files)}")
    print(f"Output created: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()