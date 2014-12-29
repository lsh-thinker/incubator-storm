package edu.sjtu.se.dclab;

import java.util.Comparator;

public class ResourceComparator {
	public static class CPULoadComparator implements Comparator<SupervisorResource>{
		@Override
		public int compare(SupervisorResource o1, SupervisorResource o2) {
			if (o1.getCpuLoad() == o2.getCpuLoad()){
				return 0;
			}else if (o1.getCpuLoad() < o2.getCpuLoad()){
				return -1;
			}else{
				return 1;
			}
		}
	}
	//
	public static class MemoryFreeComparator implements Comparator<SupervisorResource>{
		@Override
		public int compare(SupervisorResource o1, SupervisorResource o2) {
			if (o1.getMemFree() == o2.getMemFree()){
				return 0;
			}else if (o1.getMemFree() > o2.getMemFree()){
				return -1;
			}else{
				return 1;
			}
		}
	}
	
	public static class DiskFreeComparator implements Comparator<SupervisorResource>{
		@Override
		public int compare(SupervisorResource o1, SupervisorResource o2) {
			if (o1.getDiskFree() == o2.getDiskFree()){
				return 0;
			}else if (o1.getDiskFree() > o2.getDiskFree()){
				return -1;
			}else{
				return 1;
			}
		}
	}
	
	public static class BytesInOutComparator implements Comparator<SupervisorResource>{
		@Override
		public int compare(SupervisorResource o1, SupervisorResource o2) {
			if (o1.getBytesInOut() == o2.getBytesInOut()){
				return 0;
			}else if (o1.getBytesInOut() < o2.getBytesInOut()){
				return -1;
			}else{
				return 1;
			}
		}
	}
	
	public static class CommonComparator implements Comparator<SupervisorResource>{
		@Override
		public int compare(SupervisorResource o1, SupervisorResource o2) {
			if (o1.getAvailableSlots() == o2.getAvailableSlots()){
				return 0;
			}else if (o1.getBytesInOut() > o2.getAvailableSlots()){
				return -1;
			}else{
				return 1;
			}
		}
	}
	
	
}
