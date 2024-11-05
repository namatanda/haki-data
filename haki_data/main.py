import argparse
import pandas as pd
from haki_data import preprocessor, analysis, utils

def main():
    parser = argparse.ArgumentParser(description="Court Case Analytics")
    parser.add_argument("--input", default="/home/stanoo/dcrt/data/API/Hc/hc_23-24_data.csv", help="Input CSV file path")
    parser.add_argument("--output", default="./output", help="Output directory path")
    args = parser.parse_args()

    # Load and process data
    raw_df = pd.read_csv(args.input)
    df = preprocessor.clean_data(raw_df)
    df = preprocessor.transform_data(df)
 
    # Perform analysis
    filed_cases = analysis.analyze_court_outcomes(df, '2023-07-01', '2024-06-30', 'registered')
    resolved_cases = analysis.analyze_court_outcomes(df, '2023-07-01', '2024-06-30', 'concluded')
    pmmu_timelines = analysis.process_case_time_limits(df, TIME_LIMITS)
    court_productivity = analysis.get_productivity(df)
    adjourned_stats = analysis.get_adjournment(df, NON_ADJOURNABLE)
    monthly_stats = analysis.get_monthly_case_stats(df, 'registered', 'concluded')
    #judgment_scheduling = analysis.determine_judgment_scheduling(df)
    merged_quarterly = get_quarterly_stats(df)

    # Save results
    utils.save_results(args.output, {
        'filed_cases': filed_cases,
        'resolved_cases': resolved_cases,
        'monthly_stats': monthly_stats,
        # 'average_time_to_conclude': average_time_to_conclude,
        'pmmu_timelines': pmmu_timelines,
        'court_productivity': court_productivity,
        'adjourned_stats': adjourned_stats,
        
    })

    print(f"Analysis complete. Results saved to {args.output}")

if __name__ == "__main__":
    main()
